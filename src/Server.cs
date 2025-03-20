using System.Net;
using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src;

// Uncomment this block to pass the first stage
public class Server
{
    private static Dictionary<string, string> dataStore = [];
    private static string RDBFileDirectory = string.Empty;
    private static string RDBFileName = string.Empty;
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

        LoadRdbFile(Path.Combine(RDBFileDirectory, RDBFileName));
        TcpListener server = new TcpListener(IPAddress.Any, 6379);
        server.Start();

        while (true) // Keep the server running
        {
            Socket clientSocket = server.AcceptSocket(); // wait for client
                                                         // Handle each client in a separate task
            _ = Task.Run(() => HandleClient(clientSocket));
        }
    }

    private static void LoadRdbFile(string path)
    {
        try
        {
            byte[] rdbData = File.ReadAllBytes(path);
            Console.WriteLine("RDB Data: " + rdbData.Length);
            // Check REDIS magic string and version
            if (rdbData.Length < 9 ||
                Encoding.ASCII.GetString(rdbData, 0, 5) != "REDIS")
            {
                Console.WriteLine("Invalid RDB file format");
                return;
            }
            // Skip the REDIS version (next 4 bytes after REDIS)
            int position = 9;
            while (position < rdbData.Length)
            {
                byte opcode = rdbData[position];
                position++;
                if (opcode == 0xFF) // EOF
                {
                    break;
                }
                if (opcode == 0xFA) // Auxiliary field
                {
                    // Skip auxiliary field
                    string? auxKey = ReadString(rdbData, ref position);
                    string? auxValue = ReadString(rdbData, ref position);
                    continue;
                }
                if (opcode == 0xFE) // Database Selector
                {
                    position++; // Skip the database number
                    continue;
                }
                // Handle regular key-value pair (no expiry)
                if (opcode == 0xFB) // Hash table sizes
                {
                    // Read total hash table size
                    int totalSize = ReadLength(rdbData, ref position);
                    Console.WriteLine($"Total hash table size: {totalSize}");
                    // Read expiry hash table size
                    int expirySize = ReadLength(rdbData, ref position);
                    Console.WriteLine($"Expiry hash table size: {expirySize}");
                    // Now read the key-value pairs
                    for (int i = 0; i < totalSize; i++)
                    {
                        byte keyType = rdbData[position];
                        position++;
                        Console.WriteLine($"Key type: 0x{keyType:X2}");
                        string? key = ReadString(rdbData, ref position);
                        string? value = ReadString(rdbData, ref position);
                        Console.WriteLine("Key: " + key);
                        Console.WriteLine("Value: " + value);
                        if (key != null && value != null)
                        {
                            if (dataStore.ContainsKey(key))
                            {
                                dataStore[key] = value;
                                Console.WriteLine($"Loaded key: {key} with value: {value}");
                                continue;
                            }
                            dataStore.Add(key, value);
                            Console.WriteLine($"Loaded key: {key} with value: {value}");
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading RDB file: {ex.Message}");
        }
    }
    private static int ReadLength(byte[] data, ref int position)
    {
        if (position >= data.Length)
            return -1;
        byte firstByte = data[position];
        position++;
        // Check for special format
        if ((firstByte & 0xC0) == 0) // Length is encoded in the first 6 bits
        {
            return firstByte;
        }
        else if ((firstByte & 0xE0) == 0xC0) // 110xxxxx - 13 bit number
        {
            if (position >= data.Length)
                return -1;
            int next = data[position];
            position++;
            return ((firstByte & 0x1F) << 8) | next;
        }
        else if ((firstByte & 0xF0) == 0xE0) // 1110xxxx - 32 bit number
        {
            if (position + 4 > data.Length)
                return -1;
            int value = 0;
            for (int i = 0; i < 4; i++)
            {
                value = (value << 8) | data[position + i];
            }
            position += 4;
            return value;
        }
        return -1; // Invalid length encoding
    }
    private static string? ReadString(byte[] data, ref int position)
    {
        // First read and skip the value type byte
        int length = ReadLength(data, ref position);
        if (length < 0 || position + length > data.Length)
            return null;
        string str = Encoding.ASCII.GetString(data, position, length);
        position += length;
        return str;
    }

    static string BuildArrayString(string[] args)
    {
        var answer = string.Format("*{0}\r\n", args.Length);
        foreach (var item in args)
        {
            answer += string.Format("${0}\r\n{1}\r\n", item.Length, item);
        }
        return answer;
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
                        if (dataStore.TryGetValue(request[4], out string? value))
                        {
                            response = $"${value.Length}\r\n{value}\r\n";
                        }
                        else
                        {
                            response = "$-1\r\n";
                        }
                        break;
                    case "SET":
                        dataStore.Add(request[4], request[6]);
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
                                response = $"*2\r\n$3\r\ndir\r\n${RDBFileDirectory.Length}\r\n{RDBFileDirectory}\r\n";
                            }
                            else
                            {
                                response = $"*2\r\n$10\r\ndbfilename\r\n${RDBFileName.Length}\r\n{RDBFileName}\r\n";
                            }
                        }
                        break;
                    case "KEYS":
                        string pattern = request[4];
                        var keys = dataStore.Keys.Where(k => k.Contains(pattern) || pattern == "*").ToArray();
                        response = BuildArrayString(keys);
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
        dataStore.Remove(key);
    }
}