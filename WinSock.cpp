#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <iostream>
#include <ctime>
#include <stdexcept>
#include <winsock2.h>
#include <mswsock.h>
#include <Ws2tcpip.h>

enum class Switches
{
	Invalid,
	Host,
	Port,
	Delay,
	Timeout,
	GetOverlappedResult
};
static std::map<std::string, Switches> gSwitches = {
	{"-h",Switches::Host},
	{"-p",Switches::Port},
	{"-d",Switches::Delay},
	{"-t",Switches::Timeout},
	{"-gor",Switches::GetOverlappedResult}
};

enum class SocketState
{
	Accepted,
	Connected,
	BytesSent,
	BytesRead
};

static std::string gStates[] = {
	"Accepted",
	"Connected",
	"Bytes Sent",
	"Bytes Read"
};

typedef std::shared_ptr<std::vector<char>> Buffer;
struct IoData
{
public:
	IoData(SocketState state, SOCKET s, Buffer b) :
		state(state),
		socket(s),
		buffer(b)
	{
		ZeroMemory(&ol, sizeof(ol));
	}
	SOCKET socket;
	WSAOVERLAPPED ol;
	SocketState state;
	Buffer buffer;
};

struct ServerConfig
{
public:
	std::string host;
	std::string port;
};

enum class WhichBuffer
{
	Accept,
	Send,
	Receive
};

constexpr unsigned int DEMO_TIMEOUT = 30000;
constexpr unsigned int SERVER_TIMEOUT = 10000;
constexpr unsigned int DEFAULT_BUFSIZE = 1024;

static uint32_t gDelay = 0;		// default to immediate response
static uint32_t gTimeout = 0;	// default to infinite
static bool gGetOverlappedResult = false;

static PTP_WAIT gAcceptsTpW = nullptr;
static HANDLE gTaskCompleteEvt = NULL;
static HANDLE gAcceptsBeginEvt = NULL;
static HANDLE gAcceptsReadyEvt = NULL;
static std::mutex gIoMtx;

// each socket needs its own threadpool and send/receive buffers
static std::map<SOCKET, PTP_IO> gIoTp;

// only need 1 listen socket
static SOCKET gListenSocket = INVALID_SOCKET;
// demo only needs 1 accept socket
static SOCKET gAcceptSocket = INVALID_SOCKET;

std::atomic<LPFN_ACCEPTEX> acceptEx(NULL);
std::atomic<LPFN_CONNECTEX> connectEx(NULL);

void CALLBACK acceptWaitCallback(
	PTP_CALLBACK_INSTANCE Instance,
	PVOID Context,
	PTP_WAIT Wait,
	TP_WAIT_RESULT WaitResult);

void CALLBACK ioCompletionCallback(
	PTP_CALLBACK_INSTANCE Instance,
	PVOID Context,
	PVOID Overlapped,
	ULONG IoResult,
	ULONG_PTR NumberOfBytesTransferred,
	PTP_IO Io);

std::string preamble(const char* func)
{
	std::time_t tNow = std::time(0);
	std::tm lNow;
	::localtime_s(&lNow, &tNow);
	return std::string(
		std::to_string(lNow.tm_hour) + ":" + std::to_string(lNow.tm_min) + ":" + std::to_string(lNow.tm_sec)
		+ " [" + std::to_string(GetThreadId(GetCurrentThread())) + "] "
		+ func + " << ");
}

std::string errorCodeToString(DWORD errorCode)
{
	LPSTR buf = nullptr;
	FormatMessageA(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
		nullptr,
		errorCode,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPSTR)&buf,
		0,
		NULL);
	std::string r = buf ? std::string(buf) : "failed to format the errorCode";
	if (buf) LocalFree(buf);

	return r;
}

class MyExt
{
public:
	static SOCKET socket(std::string host, std::string port, bool bound)
	{
		SOCKET socket = INVALID_SOCKET;
		addrinfo hints;
		ZeroMemory(&hints, sizeof(addrinfo));
		hints.ai_family = AF_INET;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;
		hints.ai_flags = bound ? AI_PASSIVE : 0;

		PADDRINFOA infoList = nullptr;
		// Passing NULL for pNodeName should return INADDR_ANY
		if (getaddrinfo(host.empty() ? NULL : host.c_str(), port.c_str(), &hints, &infoList) != 0)
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "getaddrinfo() failed: " << WSAGetLastError() << std::endl;
			return INVALID_SOCKET;
		}

		PADDRINFOA info = infoList;
		while (socket == INVALID_SOCKET && info != nullptr)
		{
			socket = WSASocket(info->ai_family, info->ai_socktype, info->ai_protocol, nullptr, 0, WSA_FLAG_OVERLAPPED);
			std::lock_guard<std::mutex> log(gIoMtx);
			if (socket == INVALID_SOCKET)
				std::cerr << preamble(__func__) << "failed to create socket: " << WSAGetLastError() << std::endl;
			else if (bound && bind(socket, info->ai_addr, static_cast<int>(info->ai_addrlen)) == SOCKET_ERROR)
				std::cerr << preamble(__func__) << "failed to bind socket: " << WSAGetLastError() << std::endl;
			else
				break;

			info = info->ai_next;
		}

		if (socket == INVALID_SOCKET)
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "socket creation failed: " << WSAGetLastError() << std::endl;
		}

		freeaddrinfo(infoList);
		return socket;
	}

	static bool AcceptEx(
		SOCKET sListenSocket,
		SOCKET sAcceptSocket,
		PVOID lpOutputBuffer,
		DWORD dwReceiveDataLength,
		DWORD dwLocalAddressLength,
		DWORD dwRemoteAddressLength,
		LPDWORD lpdwBytesReceived,
		LPOVERLAPPED lpOverlapped)
	{
		bool failed = false;
		if (!acceptEx.load())
		{
			LPFN_ACCEPTEX new_acceptEx = NULL;
			LPFN_ACCEPTEX expected_acceptEx = NULL;
			GUID guidAcceptEx = WSAID_ACCEPTEX;
			DWORD dwCbBytes = 0;

			if (WSAIoctl(
				sListenSocket,
				SIO_GET_EXTENSION_FUNCTION_POINTER,
				&guidAcceptEx,
				sizeof(guidAcceptEx),
				&new_acceptEx,
				sizeof(new_acceptEx),
				&dwCbBytes,
				NULL,
				NULL) == SOCKET_ERROR)
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cerr << preamble(__func__) << "WSAIoctl() failed to get AcceptEx() address: " << WSAGetLastError() << std::endl;
				return false;
			}
			else if (!acceptEx.compare_exchange_strong(expected_acceptEx, new_acceptEx))
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cerr << preamble(__func__) << "failed to update AcceptEx(): " << WSAGetLastError() << std::endl;
				return false;
			}
		}

		return (*acceptEx)(sListenSocket, sAcceptSocket, lpOutputBuffer, dwReceiveDataLength, dwLocalAddressLength, dwRemoteAddressLength, lpdwBytesReceived, lpOverlapped);
	}

	static bool ConnectEx(
		SOCKET s,
		const sockaddr* name,
		int namelen,
		PVOID lpSendBuffer,
		DWORD dwSendDataLength,
		LPDWORD lpdwBytesSent,
		LPOVERLAPPED lpOverlapped)
	{
		if (!connectEx.load())
		{
			LPFN_CONNECTEX new_connectEx = NULL;
			LPFN_CONNECTEX expected_connectEx = NULL;
			GUID guidConnectEx = WSAID_CONNECTEX;
			DWORD dwCbBytes = 0;

			if (WSAIoctl(
				s,
				SIO_GET_EXTENSION_FUNCTION_POINTER,
				&guidConnectEx,
				sizeof(guidConnectEx),
				&new_connectEx,
				sizeof(new_connectEx),
				&dwCbBytes,
				NULL,
				NULL) == SOCKET_ERROR)
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cerr << preamble(__func__) << "WSAIoctl() failed to get ConnectEx() address: " << WSAGetLastError() << std::endl;
				return false;
			}
			else if (!connectEx.compare_exchange_strong(expected_connectEx, new_connectEx))
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cerr << preamble(__func__) << "failed to update ConnectEx(): " << WSAGetLastError() << std::endl;
				return false;
			}
		}

		return (*connectEx)(s, name, namelen, lpSendBuffer, dwSendDataLength, lpdwBytesSent, lpOverlapped);
	}

	static bool SetBlockingRead(SOCKET s)
	{
		u_long truth = 1;
		int r = ioctlsocket(s, FIONBIO, &truth);
		if (r == SOCKET_ERROR)
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "ioctlsocket failed: " << WSAGetLastError() << std::endl;
			return false;
		}
		else
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "ioctlsocket succeeded: " << std::endl;
			std::cout << "\tSocket: " << s << std::endl;
			std::cout << "\tOption: " << FIONBIO << std::endl;
			std::cout << "\tValue: " << truth << std::endl;
		}
		return true;
	}

	static bool SetSocketTimeout(SOCKET s, uint32_t timeout)
	{
		bool success = true;
		// set both send and recv timeout the same
		if (setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout)) == SOCKET_ERROR)
		{
			success = false;
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "setsockopt failed: " << WSAGetLastError() << std::endl;
		}
		else
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "setsockopt succeeded: " << std::endl;
			std::cout << "\tSocket: " << s << std::endl;
			std::cout << "\tOption: " << SO_SNDTIMEO << std::endl;
			std::cout << "\tValue: " << timeout << std::endl;
		}

		if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout)) == SOCKET_ERROR)
		{
			success = false;
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "setsockopt failed: " << WSAGetLastError() << std::endl;
		}
		else
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "setsockopt succeeded: " << std::endl;
			std::cout << "\tSocket: " << s << std::endl;
			std::cout << "\tOption: " << SO_RCVTIMEO << std::endl;
			std::cout << "\tValue: " << timeout << std::endl;
		}

		return success;
	}

	static bool Poll(SOCKET s)
	{
		WSAPOLLFD pfd;
		ZeroMemory(&pfd, sizeof(pfd));
		pfd.fd = s;
		pfd.events = POLLIN;
		WSAPOLLFD pfds[] = { pfd };
		int nEvents = WSAPoll(pfds, 1, gTimeout);
		if (nEvents == SOCKET_ERROR)
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "WSAPoll failed: " << WSAGetLastError() << std::endl;
			return false;
		}
		else
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "WSAPoll events: " << nEvents << std::endl;
		}
		return true;
	}
};

SOCKET connect(std::string host, std::string port)
{
	// ConnectEx requires creating a bound socket to 0.0.0.0:0 first
	SOCKET socket = MyExt::socket("", "0", true);
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "connect socket " << socket << " created" << std::endl;
	}

	bool cancelThreadPool = false;
	bool closeSocket = false;
	addrinfo hints;
	ZeroMemory(&hints, sizeof(addrinfo));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	hints.ai_flags = 0;

	PADDRINFOA infoList = nullptr;
	if (getaddrinfo(host.c_str(), port.c_str(), &hints, &infoList) != 0)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "getaddrinfo() failed: " << WSAGetLastError() << std::endl;
		return INVALID_SOCKET;
	}

	PADDRINFOA info = infoList;
	while (info != nullptr)
	{
		gIoTp[socket] = CreateThreadpoolIo((HANDLE)socket, ioCompletionCallback, nullptr, nullptr);
		if (!gIoTp[socket])
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "failed to create ThreadPoolIo: " << GetLastError() << std::endl;
			closeSocket = true;
			break;
		}
		else
		{
			Buffer bufv(new std::vector<char>(DEFAULT_BUFSIZE));
			WSABUF wsaBuf = { bufv->capacity(), bufv->data() };
			IoData* ioData = new IoData(SocketState::Connected, socket, bufv);
			DWORD dwSent = 0;

			StartThreadpoolIo(gIoTp[socket]);
			if (MyExt::ConnectEx(socket, info->ai_addr, (int)(info->ai_addrlen), &wsaBuf.buf, wsaBuf.len, &dwSent, &ioData->ol) ||
				WSAGetLastError() == WSA_IO_PENDING)
			{
				{
					std::lock_guard<std::mutex> log(gIoMtx);
					std::cout << preamble(__func__) << "connection established" << std::endl;
					if (setsockopt(socket, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0) == SOCKET_ERROR)
						std::cerr << preamble(__func__) << "failed to set connect context for socket; ignoring" << WSAGetLastError() << std::endl;
				}
				closeSocket = false;
				break;
			}
			else
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cerr << preamble(__func__) << "failed to connect socket: " << WSAGetLastError() << std::endl;
				CancelThreadpoolIo(gIoTp[socket]);
				closeSocket = true;
				continue;
			}
		}

		info = info->ai_next;
	}

	if (socket == INVALID_SOCKET)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "socket creation failed: " << WSAGetLastError() << std::endl;
	}
	else if (!info)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to locate socket address: " << WSAGetLastError() << std::endl;
		socket = INVALID_SOCKET;
	}

	if (closeSocket)
	{
		closesocket(socket);
		socket = INVALID_SOCKET;
	}

	return socket;
}

bool send(SOCKET s, std::string msg)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tSocket: " << s << std::endl;
	}

	// server requires <DWORD::size><data>
	DWORD size = msg.length();
	DWORD payload_size = sizeof(size) + size;

	Buffer bufv(new std::vector<char>(DEFAULT_BUFSIZE));
	*((DWORD*)bufv->data()) = htonl(size);
	memcpy_s(bufv->data() + sizeof(size), size, msg.c_str(), size);

	WSABUF wsaBuf = { bufv->capacity(), bufv->data() };
	WSABUF bufs[] = { wsaBuf };
	IoData* ioData = new IoData(SocketState::BytesSent, s, bufv);
	DWORD sendBytes = 0;
	DWORD dwFlags = 0;

	StartThreadpoolIo(gIoTp[s]);
	if (WSASend(s, bufs, 1, &sendBytes, dwFlags, &ioData->ol, NULL) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		switch (WSAGetLastError())
		{
		case WSA_IO_PENDING:
			std::cout << preamble(__func__) << "asynchronous" << std::endl;
			break;

		default:
			std::cerr << preamble(__func__) << "WSASend() failed: " << WSAGetLastError() << std::endl;
			CancelThreadpoolIo(gIoTp[s]);
			return false;
		}
	}
	else
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "synchronous - " << sendBytes << " bytes sent" << std::endl;
	}
	return true;
}

bool read(SOCKET s)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tSocket: " << s << std::endl;
	}

	Buffer bufv(new std::vector<char>(DEFAULT_BUFSIZE));
	WSABUF wsaBuf = { bufv->capacity(), bufv->data() };
	WSABUF bufs[] = { wsaBuf };
	IoData* ioData = new IoData(SocketState::BytesRead, s, bufv);
	DWORD readBytes = 0;
	DWORD dwFlags = 0;

	StartThreadpoolIo(gIoTp[s]);
	if (WSARecv(s, bufs, 1, &readBytes, &dwFlags, &ioData->ol, NULL) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		switch (WSAGetLastError())
		{
		case WSA_IO_PENDING:
			std::cout << preamble(__func__) << "asynchronous" << std::endl;
			break;

		default:
			std::cerr << preamble(__func__) << "WSARecv() failed: " << WSAGetLastError() << std::endl;
			CancelThreadpoolIo(gIoTp[s]);
			return false;
		}
	}
	else
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "synchronous - " << readBytes << " read" << std::endl;
	}

	if (gGetOverlappedResult)
	{
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "wait until I/O occurs or we timeout..." << std::endl;
		}
		DWORD bytesTransferred = 0;
		if (!GetOverlappedResultEx((HANDLE)s, &ioData->ol, &bytesTransferred, gTimeout, true))
		{
			DWORD e = GetLastError();
			std::lock_guard<std::mutex> log(gIoMtx);
			switch (e)
			{
			case WAIT_IO_COMPLETION:
				std::cout << preamble(__func__) << "read activity is forthcoming" << std::endl;
				break;
			case WAIT_TIMEOUT:
				// we hit our timeout, cancel the I/O
				CancelIoEx((HANDLE)s, &ioData->ol);
				break;
			default:
				std::cerr << preamble(__func__) << "GetOverlappedResult error is unhandled: " << e << std::endl;
			}
		}
		else
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "GetOverlappedResult success: " << bytesTransferred << std::endl;
		}
	}

	return true;
}

void server_main(std::string host, std::string port)
{
	gAcceptsBeginEvt = CreateEvent(NULL, FALSE, FALSE, NULL);
	if (gAcceptsBeginEvt == NULL)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create begin accepts event" << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	ServerConfig sc;
	sc.host = host;
	sc.port = port;
	gAcceptsTpW = CreateThreadpoolWait(acceptWaitCallback, &sc, NULL);
	if (gAcceptsTpW == NULL)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create accept thread pool: " << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	SetThreadpoolWait(gAcceptsTpW, gAcceptsBeginEvt, NULL);
	// this is where some unrelated initialization would go
	SetEvent(gAcceptsBeginEvt);

	// give everything enough time to work itself out
	DWORD why = WaitForSingleObject(gTaskCompleteEvt, DEMO_TIMEOUT);
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "simulated server shutting down...";
		switch (why)
		{
		case WAIT_OBJECT_0:
			std::cout << "because we're done." << std::endl;
			break;
		case WAIT_TIMEOUT:
			std::cout << "because we timed out." << std::endl;
			break;
		default:
			std::cout << "because we reached an unexpected state." << std::endl;
		}
	}

	WaitForThreadpoolWaitCallbacks(gAcceptsTpW, true);
	CloseThreadpoolWait(gAcceptsTpW);

	if (gAcceptSocket != INVALID_SOCKET)
	{
		if (gIoTp[gAcceptSocket])
		{
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cout << preamble(__func__) << "waiting for accept threadpool to finish" << std::endl;
			}
			WaitForThreadpoolIoCallbacks(gIoTp[gAcceptSocket], true);
			CloseThreadpoolIo(gIoTp[gAcceptSocket]);
		}
		closesocket(gAcceptSocket);
		gAcceptSocket = INVALID_SOCKET;
	}

	if (gListenSocket != INVALID_SOCKET)
	{
		if (gIoTp[gListenSocket])
		{
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cout << preamble(__func__) << "waiting for listen threadpool to finish" << std::endl;
			}
			WaitForThreadpoolIoCallbacks(gIoTp[gListenSocket], true);
			CloseThreadpoolIo(gIoTp[gListenSocket]);
		}
		closesocket(gListenSocket);
		gListenSocket = INVALID_SOCKET;
	}

	CloseHandle(gAcceptsBeginEvt);

	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "server thread has terminated." << std::endl;
	}
}

int main(int argc, char* argv[])
{
	std::string host = "localhost";
	std::string port;
	std::string delay;
	std::string timeout;

	for (int i = 0; i < argc; i++)
	{

		switch (gSwitches[argv[i]])
		{
		case Switches::Host:
			host = std::string(argv[++i]);
			break;
		case Switches::Port:
			port = std::string(argv[++i]);
			break;
		case Switches::Delay:
			delay = std::string(argv[++i]);
			break;
		case Switches::Timeout:
			timeout = std::string(argv[++i]);
			break;
		case Switches::GetOverlappedResult:
			gGetOverlappedResult = true;
			break;
		default:
			std::cerr
				<< preamble(__func__)
				<< "Unknown " << (std::string("-").compare(0, 1, argv[i]) == 0 ? "switch" : "value") << ": "
				<< argv[i] << std::endl;
		}
	}

	std::cout << preamble(__func__) << "Host: " << host << ", Port: " << port << std::endl;
	std::cout << "Delay: " << delay << std::endl;
	std::cout << "Timeout: " << timeout << std::endl;
	std::cout << "Get Overlapped Result: " << gGetOverlappedResult << std::endl;

	if (port.empty())
	{
		std::cerr << preamble(__func__) << "missing host (-p) parameter" << std::endl;
		ExitProcess(__LINE__);
	}

	try
	{
		if (!delay.empty()) gDelay = std::stoi(delay);
	}
	catch (std::exception const& ex)
	{
		std::cerr << preamble(__func__) << "inappropriate delay (-d) parameter: " << ex.what() << std::endl;
		ExitProcess(__LINE__);
	}

	try
	{
		if (!timeout.empty()) gTimeout = std::stoi(timeout);
	}
	catch (std::exception const& ex)
	{
		std::cerr << preamble(__func__) << "inappropriate timeout (-t) parameter: " << ex.what() << std::endl;
		ExitProcess(__LINE__);
	}

	gTaskCompleteEvt = CreateEvent(NULL, FALSE, FALSE, L"DONE");
	if (gTaskCompleteEvt == NULL)
	{
		std::cerr << preamble(__func__) << "could not create syncrhonization object for job completion: " << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	gAcceptsReadyEvt = CreateEvent(NULL, FALSE, FALSE, NULL);
	if (gAcceptsReadyEvt == NULL)
	{
		std::cerr << preamble(__func__) << "failed to create accepts ready event" << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	{
		WSADATA wd = { 0, };
		if (WSAStartup(WINSOCK_VERSION, &wd) != 0)
		{
			std::cerr << preamble(__func__) << "WSAStartup failed: " << WSAGetLastError() << std::endl;
			ExitProcess(__LINE__);
		}
	}

	// launch the 'server'; all subsequent logging needs mutex protection to look pretty
	std::thread serverThread(server_main, host, port);

	// wait for the server to register itself/get into an accepting state
	DWORD ready = WaitForSingleObject(gAcceptsReadyEvt, SERVER_TIMEOUT);
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		switch (ready)
		{
		case WAIT_OBJECT_0:
			std::cout << preamble(__func__) << "server is ready for connections, attempting to connect..." << std::endl;
			break;
		case WAIT_TIMEOUT:
			std::cerr << preamble(__func__) << "server took too long to be ready to accept connections, terminating" << std::endl;
			ExitProcess(__LINE__);
		default:
			std::cerr << preamble(__func__) << "server failed to reach a state to accept connections, not sure what is going on...terminating" << std::endl;
			ExitProcess(__LINE__);
		}
	}

	SOCKET s = connect(host, port);
	if (s == INVALID_SOCKET)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to open socket" << std::endl;
		ExitProcess(__LINE__);
	}

	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "waiting for demonstration to finish" << std::endl;
	}
	serverThread.join();

	if (gIoTp[s])
	{
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "waiting for client threadpool to finish" << std::endl;
		}
		WaitForThreadpoolIoCallbacks(gIoTp[s], true);
		CloseThreadpoolIo(gIoTp[s]);
	}

	closesocket(s);
	s = INVALID_SOCKET;

	WSACleanup();

	CloseHandle(gTaskCompleteEvt);
	CloseHandle(gAcceptsReadyEvt);

	// TODO: track all them IoData allocations so I can free them; small memory leak in a demo program is fine for now

	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "main thread has terminated." << std::endl;
	}

	return 0;
}

void sayHi(SOCKET s)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tSocket: " << s << std::endl;
	}

	if (!send(s, "hello"))
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "send failed" << std::endl;
	}
}

void sayBye(SOCKET s)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tSocket: " << s << std::endl;
	}

	if (!send(s, "goodbye"))
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "send failed" << std::endl;
	}
}

void echo(SOCKET s)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tSocket: " << s << std::endl;
	}

	if (!read(s))
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "read failed" << std::endl;
	}
}

std::string connection_details(SOCKET s)
{
	std::string details;
	char buf[INET6_ADDRSTRLEN] = { 0, };
	sockaddr_in6 addr6;
	ZeroMemory(&addr6, sizeof(addr6));
	int size = sizeof(addr6);

	if (getpeername(s, (sockaddr*)(&addr6), &size) == 0)
	{
		if (size == sizeof(sockaddr_in6))
		{
			inet_ntop(AF_INET6, &addr6.sin6_addr, buf, INET6_ADDRSTRLEN);
			details = std::string(buf);
			details += ":" + std::to_string(ntohs(addr6.sin6_port));
		}
		else if (size == sizeof(sockaddr_in))
		{
			sockaddr_in* pAddr4 = (sockaddr_in*)(&addr6);
			inet_ntop(AF_INET, &pAddr4->sin_addr, buf, INET_ADDRSTRLEN);
			details = std::string(buf);
			details += ":" + std::to_string(ntohs(pAddr4->sin_port));
		}
	}

	return details;
}

void OnAccept(IoData* ioData, ULONG_PTR NumberOfBytesTransferred)
{
	// The socket sAcceptSocket does not inherit the properties of the socket associated with sListenSocket parameter until SO_UPDATE_ACCEPT_CONTEXT is set on the socket.
	if (setsockopt(ioData->socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&gListenSocket, sizeof(gListenSocket)) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "setsockopt() for AcceptEx() failed: " << WSAGetLastError() << std::endl;
	}
	else
	{
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "connection accepted " << connection_details(ioData->socket) << ", server wishes to echo the message it receives..." << std::endl;
		}
		echo(ioData->socket);
	}
}

void OnConnect(IoData* ioData, ULONG_PTR NumberOfBytesTransferred)
{
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cout << preamble(__func__) << "connection established " << connection_details(ioData->socket) << ", say hello..." << std::endl;

	}

	sayHi(ioData->socket);
	echo(ioData->socket);
}

void OnRecv(IoData* ioData, ULONG_PTR NumberOfBytesTransferred)
{
	if (NumberOfBytesTransferred > sizeof(DWORD))
	{
		// server sends <DWORD::size><data>
		char* p = ioData->buffer->data();
		DWORD size = htonl(*(DWORD*)p);
		std::string msg = std::string(p + sizeof(size), size);
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "Recv buffer (" << size << "): " << msg << std::endl;
		}

		int cmp = msg.compare("hello");
		if (msg == "hello")
		{
			if (gDelay > 0) Sleep(gDelay);
			sayBye(ioData->socket);
		}
		else if (msg == "goodbye")
		{
			{
				std::lock_guard<std::mutex> log(gIoMtx);
				std::cout << preamble(__func__) << "goodbye received, shut the demo down" << std::endl;
			}
			SetEvent(gTaskCompleteEvt);
		}
	}
	else
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "Received message, but payload is incorrect" << std::endl;
	}
}

void OnSend(IoData* ioData, ULONG_PTR NumberOfBytesTransferred)
{
	if (NumberOfBytesTransferred > sizeof(DWORD))
	{
		// client sends <DWORD::size><data>
		char* p = ioData->buffer->data();
		DWORD size = ntohl(*(DWORD*)p);
		std::string msg = std::string(p + sizeof(size), size);
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "Send buffer (" << size << "): " << msg << std::endl;
		}
	}
	else
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "Sent message, but payload is incorrect" << std::endl;
	}
}

static void CALLBACK ioCompletionCallback(
	PTP_CALLBACK_INSTANCE Instance,
	PVOID Context,
	PVOID Overlapped,
	ULONG IoResult,
	ULONG_PTR NumberOfBytesTransferred,
	PTP_IO Io)
{
	IoData* ioData = CONTAINING_RECORD(Overlapped, IoData, IoData::ol);

	{
		std::lock_guard<std::mutex> log(gIoMtx);

		std::cout << preamble(__func__) << std::endl;
		std::cout << "\tState: " << gStates[(int)ioData->state] << std::endl;
		std::cout << "\tSocket: " << ioData->socket << std::endl;
		std::string a = errorCodeToString(IoResult);
		if (IoResult != 0)
			std::cerr << "\tError: [" << IoResult << "] " << errorCodeToString(IoResult) << std::endl;
		else
			std::cout << "\tNumber bytes transferred: " << NumberOfBytesTransferred << std::endl;
	}

	// this is a valid 'success' end condition if we've force cancelled the IO
	if (ioData->state == SocketState::BytesRead && IoResult == ERROR_OPERATION_ABORTED)
		SetEvent(gTaskCompleteEvt);

	switch (ioData->state)
	{
	case SocketState::Accepted:
		OnAccept(ioData, NumberOfBytesTransferred);
		break;
	case SocketState::Connected:
		OnConnect(ioData, NumberOfBytesTransferred);
		break;
	case SocketState::BytesSent:
		OnSend(ioData, NumberOfBytesTransferred);
		break;
	case SocketState::BytesRead:
		OnRecv(ioData, NumberOfBytesTransferred);
		break;
	default:
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "IO callback is not handled" << std::endl;
	}
}

static void CALLBACK acceptWaitCallback(
	PTP_CALLBACK_INSTANCE Instance,
	PVOID Context,
	PTP_WAIT Wait,
	TP_WAIT_RESULT WaitResult)
{
	ResetEvent(gAcceptsBeginEvt);

	if (!CallbackMayRunLong(Instance))
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "CallbackMayRunLong failed, expect odd behavior" << std::endl;
	}

	ServerConfig* sc = reinterpret_cast<ServerConfig*>(Context);

	gListenSocket = MyExt::socket("", sc->port, true);
	if (gListenSocket == INVALID_SOCKET)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create listening socket" << std::endl;
		ExitProcess(__LINE__);
	}
	std::cout << preamble(__func__) << "listening socket " << gListenSocket << " created" << std::endl;

	BOOL optval = FALSE;
	if (setsockopt(gListenSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&optval, sizeof(optval)) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "setsockopt() failed with SO_REUSEADDR: " << WSAGetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	if (setsockopt(gListenSocket, SOL_SOCKET, SO_CONDITIONAL_ACCEPT, (char*)&optval, sizeof(optval)) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "setsockopt() failed with SO_CONDITIONAL_ACCEPT: " << WSAGetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	gIoTp[gListenSocket] = CreateThreadpoolIo((HANDLE)gListenSocket, ioCompletionCallback, nullptr, nullptr);
	if (!gIoTp[gListenSocket])
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create ThreadPoolIo: " << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	// the accept socket should be an unbound INADDR_ANY
	SOCKET sAccept = MyExt::socket("", "0", false);
	if (sAccept == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create accept socket" << std::endl;
		ExitProcess(__LINE__);
	}
	std::cout << preamble(__func__) << "accept socket " << sAccept << " created" << std::endl;

	gIoTp[sAccept] = CreateThreadpoolIo((HANDLE)sAccept, ioCompletionCallback, nullptr, nullptr);
	if (!gIoTp[sAccept])
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "failed to create ThreadPoolIo: " << GetLastError() << std::endl;
		ExitProcess(__LINE__);
	}

	// this seems unnecessary...
	StartThreadpoolIo(gIoTp[gListenSocket]);
	if (listen(gListenSocket, SOMAXCONN) == SOCKET_ERROR)
	{
		std::lock_guard<std::mutex> log(gIoMtx);
		std::cerr << preamble(__func__) << "listen failed: " << WSAGetLastError() << std::endl;
		closesocket(gListenSocket);
		CancelThreadpoolIo(gIoTp[gListenSocket]);
		ExitProcess(__LINE__);
	}

	Buffer bufv(new std::vector<char>(DEFAULT_BUFSIZE));
	WSABUF wsaBuf = { bufv->capacity(), bufv->data() };
	IoData* ioData = new IoData(SocketState::Accepted, sAccept, bufv);

	StartThreadpoolIo(gIoTp[sAccept]);
	if (!MyExt::AcceptEx(gListenSocket, sAccept, wsaBuf.buf, wsaBuf.len, sizeof(sockaddr_in) + 16, sizeof(sockaddr_in) + 16, NULL, &ioData->ol))
	{
		switch (WSAGetLastError())
		{
		case WSA_IO_PENDING:
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cout << preamble(__func__) << "server simulation is awaiting connection..." << std::endl;
		}
		SetEvent(gAcceptsReadyEvt);
		break;
		default:
		{
			std::lock_guard<std::mutex> log(gIoMtx);
			std::cerr << preamble(__func__) << "failed to set socket to accept state: " << WSAGetLastError() << std::endl;
		}
		CancelThreadpoolIo(gIoTp[sAccept]);
		ExitProcess(__LINE__);
		}
	}
}
