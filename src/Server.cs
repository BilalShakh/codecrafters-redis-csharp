using codecrafters_redis.src.Tools;
using System.Collections;
using System.Net;
using System.Net.Sockets;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true)
{
    Socket socket = await server.AcceptSocketAsync(); // wait for client
    HandleSocketAsync(socket);
}

async Task HandleSocketAsync(Socket socket)
{
    while (socket.Connected)
    {
        byte[] requestData = new byte[socket.ReceiveBufferSize];
        await socket.ReceiveAsync(requestData);
        RespData request = new RespData(RespDataType.String, "PING");
        RespData response;
        switch (request.Content)
        {
            case "PING":
                response = new RespData(RespDataType.String, "PONG");
                break;
            default:
                response = new RespData(RespDataType.Error, "Ocurrio un error");
                break;
        }
        await socket.SendAsync(response.GetRespStringInBytes());
    }
}
