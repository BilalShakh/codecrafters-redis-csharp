using System.Net;
using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src;

// Uncomment this block to pass the first stage
public class Server
{
    public static Dictionary<string, string> data = [];
    public static string RDBFileDirectory = string.Empty;
    public static string RDBFileName = string.Empty;
    public static void Main(string[] args)
    {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        Console.WriteLine("Logs from your program will appear here!");

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--dir":
                    if (i + 1 < args.Length)
                        RDBFileDirectory = args[i + 1];
                    break;
                case "--dbfilename":
                    if (i + 1 < args.Length)
                        RDBFileName = args[i + 1];
                    break;
            }
        }

        TcpListener server = new TcpListener(IPAddress.Any, 6379);
        server.Start();

        while (true) // Keep the server running
        {
            Socket clientSocket = server.AcceptSocket(); // wait for client
                                                         // Handle each client in a separate task
            _ = Task.Run(() => HandleClient(clientSocket));
        }
    }

    static async Task HandleClient(Socket clientSocket)
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

                string response = string.Empty;
                switch (request[2])
                {
                    case "PING":
                        response = "+PONG\r\n";
                        break;
                    case "ECHO":
                        response = $"${request[4].Length}\r\n{request[4]}\r\n";
                        break;
                    case "GET":
                        if (data.TryGetValue(request[4], out string? value))
                        {
                            response = $"${value.Length}\r\n{value}\r\n";
                        }
                        else
                        {
                            response = "$-1\r\n";
                        }
                        break;
                    case "SET":
                        data.Add(request[4], request[6]);
                        if (request.Length > 7 && request[8] == "px")
                        {
                            int timeToExpire = int.Parse(request[10]);
                            _ = HandleExpiry(timeToExpire, request[4]);
                        }
                        response = "+OK\r\n";
                        break;
                    case "CONFIG":
                        if (request[6] == "dir" || request[6] == "dbfilename")
                        {
                            if (request[6] == "dir")
                            {
                                response = $"${RDBFileDirectory.Length}\r\n{RDBFileDirectory}\r\n";
                            }
                            else
                            {
                                response = $"${RDBFileName.Length}\r\n{RDBFileName}\r\n";
                            }
                        }
                        break;
                    default:
                        response = "-ERR unknown command\r\n";
                        break;
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

    static async Task HandleExpiry(int timeToExpire, string key)
    {
        await Task.Delay(timeToExpire);
        data.Remove(key);
    }
}