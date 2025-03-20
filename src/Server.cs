using System.Net;
using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src;

// Uncomment this block to pass the first stage
public class Server
{
    public static Dictionary<string, string> dataStore = [];
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

        LoadContents();
        TcpListener server = new TcpListener(IPAddress.Any, 6379);
        server.Start();

        while (true) // Keep the server running
        {
            Socket clientSocket = server.AcceptSocket(); // wait for client
                                                         // Handle each client in a separate task
            _ = Task.Run(() => HandleClient(clientSocket));
        }
    }

    static void LoadContents()
    {
        string filePath = Path.Combine(RDBFileDirectory, RDBFileName);
        if (!File.Exists(filePath))
        {
            Console.WriteLine($"File {filePath} does not exist!");
            return;
        }
        try
        {
            byte[] data = File.ReadAllBytes(filePath);
            Console.WriteLine(
                $"File read successfully. Data (hex): {BitConverter.ToString(data)}");
            ParseRedisRdbData(data);
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"An error occurred while loading contents: {ex.Message}");
        }
    }

    static void ParseRedisRdbData(byte[] data)
    {
        int index = 0;
        try
        {
            while (index < data.Length)
            {
                if (data[index] == 0xFB) // Start of database section
                {
                    index = ParseDatabaseSection(data, index);
                    if (data[index] == 0xFF)
                    {
                        Console.WriteLine("End of database section detected.");
                        break;
                    }
                }
                else
                {
                    index++; // Skip unknown or unhandled sections
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing RDB data: {ex.Message}");
            throw;
        }
    }
    
    static int ParseDatabaseSection(byte[] data, int startIndex)
    {
        int index = startIndex + 1;
        int length = data[index] + data[index + 1];
        Console.WriteLine(
            $"Database section detected. Key-value count: {length}");
        index += 2;
        for (int i = 0; i < length; i++)
        {
            ulong expiryTimeStampFC = 0;
            uint expiryTimeStampFD = 0;
            if (data[index] == 0xFC)
            {
                index++;
                expiryTimeStampFC = ExtractUInt64(data, ref index);
                Console.WriteLine($"Extracted expiry information. Milliseconds information. Timestamp:{expiryTimeStampFC}");
            }
            if (data[index] == 0xFD)
            {
                index++;
                expiryTimeStampFD = ExtractUInt32(data, ref index);
                Console.WriteLine($"Extracted expiry information. Seconds information. Timestamp:{expiryTimeStampFD}");
            }
            if (data[index] == 0x00)
            {
                index++;
                Console.WriteLine("Skipping 0x00 byte.");
            }
            if (data[index] == 0xFF)
            {
                Console.WriteLine("End of database section detected.");
                break;
            }
            // Parse key
            int keyLength = data[index];
            Console.WriteLine($"Key length: {keyLength}");
            index++;
            string key = ParseString(data, ref index, keyLength);
            Console.WriteLine($"Parsed key: {key}");
            // Parse value
            int valueLength = data[index];
            Console.WriteLine($"Value length: {valueLength}");
            index++;
            string value = ParseString(data, ref index, valueLength);
            Console.WriteLine($"Parsed value: {value}");
            if (key.Length == 0)
            {
                Console.WriteLine("Empty key found. Skipping.");
                continue;
            }
            if (dataStore.ContainsKey(key))
            {
                dataStore[key] = value;
                Console.WriteLine($"Key-Value pair updated: {key} => {value}");
                continue;
            }
            dataStore.Add(key, value);
            Console.WriteLine($"Key-Value pair added: {key} => {value}");
            if (expiryTimeStampFC != 0)
            {
                _ = HandleTimeStampExpiry((long)expiryTimeStampFC, key, false);
                expiryTimeStampFC = 0;
            }
            else if (expiryTimeStampFD != 0)
            {
                _ = HandleTimeStampExpiry(expiryTimeStampFD, key, true);
                expiryTimeStampFD = 0;
            }
        }
        return index;
    }
    
    static string ParseString(byte[] data, ref int index, int length)
    {
        string result =
            Encoding.Default.GetString(data.Skip(index).Take(length).ToArray());
        index += length;
        return result;
    }

    static ulong ExtractUInt64(byte[] data, ref int index)
    {
        if (index + 8 >= data.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "Index out of range for extracting UInt64.");
        }
        ulong value = BitConverter.ToUInt64(data, index);
        index += 8;
        return value;
    }

    static uint ExtractUInt32(byte[] data, ref int index)
    {
        if (index + 4 >= data.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "Index out of range for extracting UInt32.");
        }
        uint value = BitConverter.ToUInt32(data, index);
        index += 4;
        return value;
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

    static async Task HandleTimeStampExpiry(long unixTimeStamp, string key, bool isSeconds)
    {
        int delay = 0;
        if (isSeconds)
        {
            delay = (int)(unixTimeStamp - DateTimeOffset.Now.ToUnixTimeSeconds()) * 1000;
        }
        else
        {
            delay = (int)(unixTimeStamp - DateTimeOffset.Now.ToUnixTimeMilliseconds());
        }
        Console.WriteLine($"Delay: {delay} unixTimeStamp:{unixTimeStamp} Now.ToUnixTimeMilliseconds:{DateTimeOffset.Now.ToUnixTimeMilliseconds()} Now.ToUnixTimeSeconds:{DateTimeOffset.Now.ToUnixTimeSeconds()}");
        if (delay < 0)
        {
            Console.WriteLine("Expiry time has already passed. Removing key.");
            dataStore.Remove(key);
            return;
        }
        await Task.Delay(delay);
        dataStore.Remove(key);
    }
 }