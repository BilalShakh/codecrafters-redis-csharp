using System.Collections;
using System.Net;
using System.Net.Sockets;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true) // Keep the server running
{
    Socket clientSocket = server.AcceptSocket(); // wait for client
                                                 // Handle each client in a separate task
    _ = Task.Run(() => HandleClient(clientSocket));
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
            Console.WriteLine("Received data: " +
                              System.Text.Encoding.ASCII.GetString(buffer));
            string response = "+PONG\r\n";
            byte[] responseBytes = System.Text.Encoding.ASCII.GetBytes(response);
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
