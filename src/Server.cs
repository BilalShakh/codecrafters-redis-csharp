using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
var clientSocket = server.AcceptSocket(); // wait for client
clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);
clientSocket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);
