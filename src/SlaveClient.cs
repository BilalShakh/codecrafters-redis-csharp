using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src
{
    class SlaveClient
    {
        private static string MasterHost = string.Empty;
        private static int MasterPort = 0;
        private static int SlaveReplicationOffset = 0;

        public SlaveClient(string masterHost, int masterPort)
        {
            MasterHost = masterHost;
            MasterPort = masterPort;
            HandleMasterHandshake();
        }

        static void HandleMasterHandshake()
        {
            if (MasterHost == string.Empty)
            {
                return;
            }
            try
            {
                TcpClient tcpClient = new TcpClient(MasterHost, MasterPort);

                NetworkStream stream = tcpClient.GetStream();

                string[] requests = [
                    Utilities.BuildArrayString(["ping"]),
                    Utilities.BuildArrayString(["REPLCONF", "listening-port", "6380"]),
                    Utilities.BuildArrayString(["REPLCONF", "capa", "psync2"]),
                    Utilities.BuildArrayString(["PSYNC", "?", "-1"]),
                ];

                foreach (var request in requests)
                {
                    byte[] data = Encoding.ASCII.GetBytes(request);
                    stream.Write(data, 0, data.Length);

                    var bytesRead = stream.Read(data, 0, data.Length);
                    var responseData = Encoding.ASCII.GetString(data, 0, bytesRead);
                }

                _ = Task.Run(() => HandleSlaveClient(tcpClient.Client));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling master PING: {ex.Message}");
            }
        }

        static async Task HandleSlaveClient(Socket clientSocket)
        {
            int key = ReplicaRegistry.Replicas.FirstOrDefault(x => x.Value == clientSocket).Key;
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
                    int index = 0;
                    if (bytesRead > 120 && Utilities.isEmptyRDB(buffer))
                    {
                        Console.WriteLine("Empty RDB file received from master.");
                        index = 119;
                    }
                    string receivedData = Encoding.ASCII.GetString(buffer, index, bytesRead).Trim();
                    Console.WriteLine($"Received {bytesRead} bytes data from master: " + receivedData);

                    var requests = ParseRESP(receivedData, out int[] requestBytes);

                    for (int i = 0; i < requests.Count; i++)
                    {
                        var request = requests[i];
                        if (request.Length == 0)
                        {
                            Console.WriteLine("Invalid RESP format.");
                            continue;
                        }

                        switch (request[0].ToUpper())
                        {
                            case "PING":
                                break;
                            case "SET":
                                if (request.Length >= 3)
                                {
                                    MasterClient.dataStore[request[1]] = request[2];
                                    Console.WriteLine($"SET command received. Key: {request[1]}, Value: {request[2]}");
                                }
                                break;
                            case "GET":
                                if (request.Length >= 2)
                                {
                                    if (MasterClient.dataStore.TryGetValue(request[1], out string? value))
                                    {
                                        await SendResponse(clientSocket, Utilities.BuildBulkString(value));
                                    }
                                    else
                                    {
                                        await SendResponse(clientSocket, "$-1\r\n");
                                    }
                                }
                                break;
                            case "REPLCONF":
                                if (request.Length >= 3)
                                {
                                    Console.WriteLine($"REPLCONF command received. Key: {request[1]}, Value: {request[2]}, ReplicaRegistryKey: {key}");
                                }
                                string replconfResponse = Utilities.BuildArrayString(["REPLCONF", "ACK", SlaveReplicationOffset.ToString()]);
                                ReplicaRegistry.ReplicasFinished[key] = requestBytes.Sum() >= bytesRead;
                                await SendResponse(clientSocket, replconfResponse);
                                break;
                            default:
                                Console.WriteLine("Unknown command received from master.");
                                break;
                        }
                        SlaveReplicationOffset += requestBytes[i];
                    }
                    ReplicaRegistry.ReplicasFinished[key] = true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling slave client: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                clientSocket.Close();
            }
        }

        static List<string[]> ParseRESP(string data, out int[] requestBytes)
        {
            var lines = data.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var result = new List<string[]>();
            var currentArray = new List<string>();
            int expectedLength = 0;
            bool inArray = false;
            var validDataStarted = false;
            List<int> bytes = [];
            int currentBytes = 0;

            //Console.WriteLine("Lines: " + string.Join(",", lines));

            for (int i = 0; i < lines.Length; i++)
            {
                if ((lines[i].StartsWith("*") || lines[i].StartsWith("$")) && lines[i].Length > 1)
                {
                    validDataStarted = true;
                }

                if (!validDataStarted)
                {
                    continue;
                }

                if (lines[i].StartsWith("*"))
                {
                    Console.WriteLine("Array detected here: " + lines[i]);
                    if (currentArray.Count > 0)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                        bytes.Add(currentBytes);
                        currentBytes = 0;
                    }
                    expectedLength = int.Parse(lines[i].Substring(1)) * 2;
                    inArray = true;
                    currentBytes += lines[i].Length + 2;
                }
                else if (lines[i].StartsWith("$"))
                {
                    Console.WriteLine("Bulk string detected here: " + lines[i]);
                    int length = int.Parse(lines[i].Substring(1));
                    currentBytes += lines[i].Length + 2;
                    if (i + 1 < lines.Length && lines[i + 1].Length == length)
                    {
                        currentArray.Add(lines[i + 1]);
                        currentBytes += lines[i + 1].Length + 2;
                        i++; // Skip the next line as it is part of the bulk string
                    }
                    if (inArray && currentArray.Count == expectedLength)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                        inArray = false;
                        bytes.Add(currentBytes);
                        currentBytes = 0;
                    }
                }
                else
                {
                    if (currentArray.Count > 0)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                        bytes.Add(currentBytes);
                        currentBytes = 0;
                    }
                }
            }

            if (currentArray.Count > 0)
            {
                result.Add(currentArray.ToArray());
                bytes.Add(currentBytes);
                currentBytes = 0;
            }

            requestBytes = bytes.ToArray();
            Console.WriteLine("Parsed RESP: " + string.Join(", ", result.Select(r => "[" + string.Join(", ", r) + "]")));
            Console.WriteLine("Request Bytes: " + string.Join(", ", requestBytes));
            return result;
        }

        static async Task SendResponse(Socket clientSocket, string response)
        {
            byte[] responseBytes = Encoding.ASCII.GetBytes(response);
            await Task.Run(() => clientSocket.Send(responseBytes));
        }
    }
}
