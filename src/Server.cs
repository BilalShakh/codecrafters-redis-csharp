using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

Dictionary<string, string> data = new Dictionary<string, string>();

while (true) // Keep the server running
{
    Socket clientSocket = server.AcceptSocket(); // wait for client
                                                 // Handle each client in a separate task
    _ = Task.Run(() => HandleClient(clientSocket));
}

async Task HandleClient(Socket clientSocket)
{
    try
    {
        while (true) // Keep connection alive
        {
            byte[] buffer = new byte[1024];
            int bytesRead = await Task.Run(() => clientSocket.Receive(buffer));
            if (bytesRead == 0) // Client disconnected
            {
                break;
            }
            string receivedData = Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim();
            var request = receivedData.Split("\r\n");
            Console.WriteLine("Received data: " + receivedData);

            string response;
            if (request[2] == "PING")
            {
                response = "+PONG\r\n";
            }
            else if (request[2] == "ECHO")
            {
                response = $"${request[4].Length}\r\n{request[4]}\r\n";
            }
            else if (request[2] == "GET")
            {
                if (data.ContainsKey(request[4]))
                {
                    response = $"${data[request[4]].Length}\r\n{data[request[4]]}\r\n";
                }
                else
                {
                    response = "$-1\r\n";
                }
            }
            else if (request[2] == "SET")
            {
                data.Add(request[4], request[6]);
                response = "+OK\r\n";
            }
            else
            {
                response = "-ERR unknown command\r\n";
            }

            byte[] responseBytes = Encoding.ASCII.GetBytes(response);
            await Task.Run(() => clientSocket.Send(responseBytes));
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error handling client: {ex.Message}");
    }
    finally
    {
        clientSocket.Close();
    }
}
