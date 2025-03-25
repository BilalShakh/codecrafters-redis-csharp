using codecrafters_redis.src.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src
{
    class MasterClient
    {
        public static readonly Dictionary<string, string> dataStore = [];
        public static readonly Dictionary<string, StreamEntry> streamStore = [];
        private static string MasterReplicationId = string.Empty;
        public static int MasterReplicationOffset = 0;
        private static string MasterHost = string.Empty;
        private static int MasterPort = 0;
        private static string RDBFileDirectory = string.Empty;
        private static string RDBFileName = string.Empty;
        private static readonly List<Socket> slaveSockets = [];
        public static readonly List<Socket> inSyncReplicas = [];
        private static int Port = 6379;


        public MasterClient(string masterHost, int masterPort, string rdbFileName, string rdbFileDirectory, int port)
        {
            MasterHost = masterHost;
            MasterPort = masterPort;
            RDBFileName = rdbFileName;
            RDBFileDirectory = rdbFileDirectory;
            Port = port;
            if (MasterHost == string.Empty)
            {
                MasterReplicationId = Utilities.Generate40CharacterGuid();
            }
            Start();
        }

        public static void Start()
        {
            TcpListener server = new(IPAddress.Any, Port);
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
                    Console.WriteLine("Master Received data: " + receivedData);

                    string response = string.Empty;
                    switch (request[2])
                    {
                        case "PING":
                            response = "+PONG\r\n";
                            break;
                        case "ECHO":
                            response = Utilities.BuildBulkString(request[4]);
                            break;
                        case "GET":
                            if (dataStore.TryGetValue(request[4], out string? value))
                            {
                                response = Utilities.BuildBulkString(value);
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
                                    response = Utilities.BuildArrayString(["dir", RDBFileDirectory]);
                                }
                                else
                                {
                                    response = Utilities.BuildArrayString(["dbfilename", RDBFileName]);
                                }
                            }
                            break;
                        case "KEYS":
                            string pattern = request[4];
                            var keys = dataStore.Keys.Where(k => k.Contains(pattern) || pattern == "*").ToArray();
                            response = Utilities.BuildArrayString(keys);
                            break;
                        case "INFO":
                            if (MasterHost != string.Empty)
                            {
                                Console.WriteLine($"MasterHost:{MasterHost} MasterPort:{MasterPort}");
                                response = Utilities.BuildBulkString($"role:slave");
                            }
                            else
                            {
                                StringBuilder info = new();
                                info.AppendLine("role:master");
                                info.AppendLine($"master_replid:{MasterReplicationId}");
                                info.AppendLine($"master_repl_offset:{MasterReplicationOffset}");
                                response = Utilities.BuildBulkString(info.ToString());
                            }
                            break;
                        case "REPLCONF":
                            response = "+OK\r\n";
                            if (request[4] == "listening-port")
                            {
                                slaveSockets.Add(clientSocket);
                            }
                            else if (request[4] == "ACK")
                            {
                                response = string.Empty;
                                Console.WriteLine("Handling Master REPLCONF ACK command.");
                                int thisAckBytes = int.Parse(request[6]);

                                if (thisAckBytes == MasterReplicationOffset)
                                {
                                    inSyncReplicas.Add(clientSocket);
                                }

                                MasterReplicationOffset += 37;
                            }
                            break;
                        case "PSYNC":
                            response = "+FULLRESYNC " + MasterReplicationId + " " + MasterReplicationOffset + "\r\n";
                            break;
                        case "WAIT":
                            Console.WriteLine("Handling WAIT command.");
                            int responseCount = await HandleWait(ParseWaitInput(request));
                            Console.WriteLine("Master Response count: " + responseCount);
                            response = Utilities.BuildIntegerString(responseCount);
                            break;
                        case "TYPE":
                            if (dataStore.ContainsKey(request[4]))
                            {
                                response = Utilities.BuildBulkString("string");
                            }
                            else if (streamStore.ContainsKey(request[4]))
                            {
                                response = Utilities.BuildBulkString("stream");
                            }
                            else
                            {
                                response = Utilities.BuildBulkString("none");
                            }
                            break;
                        case "XADD":
                            string Key = request[4];
                            if (!streamStore.ContainsKey(Key))
                            {
                                streamStore.Add(Key, new StreamEntry { Store = [] });
                            }
                            string streamEntryId = request[6];
                            if (!IsStreamEntryIdValid(streamEntryId, Key))
                            {
                                response = Utilities.BuildErrorString("The ID specified in XADD is equal or smaller than the target stream top item");
                                break;
                            }
                            streamStore[Key].Store.Add(streamEntryId, []);
                            string[][] keyValuePairs = ParseStreamKeyValuePairs(request);
                            foreach (var pair in keyValuePairs)
                            {
                                Console.WriteLine("Added Key: " + pair[0] + " Value: " + pair[1]);
                                streamStore[Key].Store[streamEntryId].Add(pair[0], pair[1]);
                            }
                            response = Utilities.BuildBulkString(request[6]);
                            break;
                        default:
                            response = "-ERR unknown command\r\n";
                            break;
                    }

                    Console.WriteLine("Master Response: " + response);
                    if (response != string.Empty)
                    {
                        byte[] responseBytes = Encoding.ASCII.GetBytes(response);
                        await Task.Run(() => clientSocket.Send(responseBytes));
                    }

                    if (request[2] == "SET")
                    {
                        string slaveResponse = Utilities.BuildArrayString(["SET", request[4], request[6]]);
                        SendToSlaves(slaveResponse);
                    }

                    if (request[2] == "PSYNC")
                    {
                        byte[] RDBBytes = CreateEmptyRDBFile();
                        await Task.Run(() => clientSocket.Send(RDBBytes));
                    }
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

        private static string[] ParseWaitInput(string[] input)
        {
            string[] parsedInput = [input[2], input[4], input[6]];
            return parsedInput;
        }

        private static string[][] ParseStreamKeyValuePairs(string[] input)
        {
            int inputLength = int.Parse(input[0].Replace("*",""));
            string[][] res = new string[(inputLength-3)/2][];
            Console.WriteLine("Input length: " + inputLength);
            int j = 0;
            for (int i = 7; i < input.Length; i+=4)
            {
                res[j] = [input[i + 1], input[i + 3]];
                j++;
            }
            Console.WriteLine("Parsed Stream Key Value Pairs: " + res);
            return res;
        }

        public static bool IsStreamEntryIdValid(string streamEntryId, string streamKey)
        {
            string[] streamEntryIds = streamStore[streamKey].Store.Keys.ToArray();
            string lastStreamEntryId = streamEntryIds[streamEntryIds.Length - 1];
            string[] lastStreamEntryIdParts = lastStreamEntryId.Split("-");
            string[] streamEntryIdParts = streamEntryId.Split("-");
            int lastStreamEntryIdTime = int.Parse(lastStreamEntryIdParts[0]);
            int streamEntryIdTime = int.Parse(streamEntryIdParts[0]);
            int lastStreamEntryIdSeq = int.Parse(lastStreamEntryIdParts[1]);
            int streamEntryIdSeq = int.Parse(streamEntryIdParts[1]);
            
            if (streamEntryId == "0-0")
            {
                return false;
            }
            
            if (streamEntryIdTime < lastStreamEntryIdTime)
            {
                return false;
            }
            
            if (streamEntryIdTime == lastStreamEntryIdTime && streamEntryIdSeq <= lastStreamEntryIdSeq)
            {
                return false;
            }

            return true;
        }

        private static async Task<int> HandleWait(string[] input)
        {
            if (input.Length != 3)
            {
                throw new Exception("Invalid input for handle wait.");
            }

            int replicaCount = int.Parse(input[1]);
            int timeout = int.Parse(input[2]);
            
            if (slaveSockets.Count == 0)
            {
                Console.WriteLine("No replicas connected.");
                return slaveSockets.Count;
            }

            if (MasterReplicationOffset == 0)
            {
                Console.WriteLine("Master has not yet received any data.");
                await Task.Delay(timeout);
                return slaveSockets.Count;
            }

            foreach (var replica in slaveSockets)
            {
                replica.ReceiveTimeout = timeout;
                SendResponse(Utilities.BuildArrayString(["REPLCONF", "GETACK", "*"]), replica);
            }

            Console.WriteLine("Waiting for replicas to catch up.");
            await Task.Delay(timeout);
            return inSyncReplicas.Count;
        }

        static void SendResponse(string response, Socket socket)
        {
            byte[] responseBytes = Encoding.ASCII.GetBytes(response);
            socket.Send(responseBytes);
        }

        static async Task HandleExpiry(int timeToExpire, string key)
        {
            await Task.Delay(timeToExpire);
            dataStore.Remove(key);
        }

        static void SendToSlaves(string data)
        {
            Console.WriteLine($"Sending data to {slaveSockets.Count} slaves: " + data);
            byte[] responseBytes = Encoding.ASCII.GetBytes(data);
            foreach (var slaveSocket in slaveSockets)
            {
                slaveSocket.Send(responseBytes);
            }
            MasterReplicationOffset += responseBytes.Length;
        }

        static byte[] CreateEmptyRDBFile()
        {
            const string emptyRdbFileBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
            byte[] rdbFile = Convert.FromBase64String(emptyRdbFileBase64);
            string rdbFileLength = $"${rdbFile.Length}\r\n";
            return Encoding.ASCII.GetBytes(rdbFileLength).Concat(rdbFile).ToArray();
        }
    }
}
