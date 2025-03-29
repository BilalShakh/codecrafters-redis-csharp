using codecrafters_redis.src.Data;
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src
{
    class MasterClient
    {
        public static readonly Dictionary<string, string> dataStore = [];
        public static readonly Dictionary<string, StreamEntry> streamStore = [];
        public static readonly Dictionary<string, StreamEntry> newlyAddedStreamStore = [];
        private static bool isBlockingRead = false;
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
                    switch (request[2].ToUpper())
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
                            response = HandleXADD(request);
                            break;
                        case "XRANGE":
                            response = HandleXRANGE(request);
                            break;
                        case "XREAD":
                            response = await Task.Run(() => HandleXRead(request));
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
                Console.WriteLine(ex.StackTrace);
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

        private static string GenerateEntireStreamEntryId(string streamKey)
        {
            string time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();

            if (streamStore[streamKey].Store.Count == 0)
            {
                return $"{time}-0";
            }

            string lastStreamEntryId = streamStore[streamKey].Store.Keys.Last();
            string[] lastStreamEntryIdParts = lastStreamEntryId.Split("-");
            int lastStreamEntryIdSeq = int.Parse(lastStreamEntryIdParts[1]);

            return lastStreamEntryIdParts[0] == time
                ? $"{time}-{lastStreamEntryIdSeq + 1}"
                : $"{time}-0";
        }

        private static string GenerateStreamEntryId(string streamEntryId, string streamKey)
        {
            string[] streamEntryIdParts = streamEntryId.Split("-");
            int streamEntryIdTime = int.Parse(streamEntryIdParts[0]);

            if (streamStore[streamKey].Store.Count == 0)
            {
                return streamEntryIdTime == 0 ? "0-1" : $"{streamEntryIdTime}-0";
            }

            string lastStreamEntryId = streamStore[streamKey].Store.Keys.Last();
            string[] lastStreamEntryIdParts = lastStreamEntryId.Split("-");
            int lastStreamEntryIdTime = int.Parse(lastStreamEntryIdParts[0]);
            int lastStreamEntryIdSeq = int.Parse(lastStreamEntryIdParts[1]);

            return streamEntryIdTime == lastStreamEntryIdTime
                ? $"{streamEntryIdTime}-{lastStreamEntryIdSeq + 1}"
                : $"{streamEntryIdTime}-0";
        }

        public static bool IsStreamEntryIdValid(string streamEntryId, string streamKey, out string errorMessage)
        {
            errorMessage = string.Empty;
            if (streamEntryId == "0-0")
            {
                errorMessage = "The ID specified in XADD must be greater than 0-0";
                return false;
            }

            string[] streamEntryIds = streamStore[streamKey].Store.Keys.ToArray();

            if (streamEntryIds.Length == 0)
            {
                return true;
            }

            string lastStreamEntryId = streamEntryIds[streamEntryIds.Length - 1];
            string[] lastStreamEntryIdParts = lastStreamEntryId.Split("-");
            string[] streamEntryIdParts = streamEntryId.Split("-");
            int lastStreamEntryIdTime = int.Parse(lastStreamEntryIdParts[0]);
            int streamEntryIdTime = int.Parse(streamEntryIdParts[0]);
            int lastStreamEntryIdSeq = int.Parse(lastStreamEntryIdParts[1]);
            int streamEntryIdSeq = int.Parse(streamEntryIdParts[1]);
            
            if (streamEntryIdTime < lastStreamEntryIdTime)
            {
                errorMessage = "The ID specified in XADD is equal or smaller than the target stream top item";
                return false;
            }
            
            if (streamEntryIdTime == lastStreamEntryIdTime && streamEntryIdSeq <= lastStreamEntryIdSeq)
            {
                errorMessage = "The ID specified in XADD is equal or smaller than the target stream top item";
                return false;
            }

            return true;
        }

        private static string HandleXADD(string[] input)
        {
            string Key = input[4];
            
            if (!streamStore.ContainsKey(Key))
            {
                streamStore.Add(Key, new StreamEntry { Store = [] });
            }

            if (isBlockingRead && !newlyAddedStreamStore.ContainsKey(Key))
            {
                newlyAddedStreamStore.Add(Key, new StreamEntry { Store = [] });
            }

            string streamEntryId = input[6];
            string errorMessage = string.Empty;
            
            if (streamEntryId == "*")
            {
                streamEntryId = GenerateEntireStreamEntryId(Key);
            } 
            else if (streamEntryId.Contains("*"))
            {
                streamEntryId = GenerateStreamEntryId(streamEntryId, Key);
            }
            
            if (!IsStreamEntryIdValid(streamEntryId, Key, out errorMessage))
            {
                return Utilities.BuildErrorString(errorMessage);
            }
            
            streamStore[Key].Store.Add(streamEntryId, []);
            if (isBlockingRead)
            {
                newlyAddedStreamStore[Key].Store.Add(streamEntryId, []);
            }
            string[][] keyValuePairs = ParseStreamKeyValuePairs(input);
            foreach (var pair in keyValuePairs)
            {
                Console.WriteLine("Added Key: " + pair[0] + " Value: " + pair[1]);
                streamStore[Key].AddToStore(streamEntryId, pair[0], pair[1]);
                if (isBlockingRead)
                {
                    newlyAddedStreamStore[Key].AddToStore(streamEntryId, pair[0], pair[1]);
                }
            }
            return Utilities.BuildBulkString(streamEntryId);
        }

        private static string HandleXRead(string[] input)
        {
            if (input.Length < 9)
            {
                throw new Exception("Invalid input for handle xread. Length: " + input.Length);
            }
            string[] keys = new string[(input.Length - 5) / 4];
            string[] starts = new string[(input.Length - 5) / 4];
            List<XReadOutput> result = [];
            int keysStartIndex = 5;
            int startsStartIndex = 5 + ((input.Length - 5) / 2);
            int blockingTime = 0;

            if (input[4] == "block")
            {
                keysStartIndex = 9;
                startsStartIndex = 9 + ((input.Length - 9) / 2);
                keys = new string[(input.Length - 9) / 4];
                starts = new string[(input.Length - 9) / 4];
                blockingTime = int.Parse(input[6]);
            }

            for (int i = keysStartIndex, j = 0; i + 1 < input.Length && j < keys.Length; i += 2, j++)
            {
                keys[j] = input[i+1];
            }

            for (int i = startsStartIndex, j = 0; i + 1 < input.Length && j < starts.Length; i += 2, j++)
            {
                starts[j] = input[i + 1];
            }

            if (blockingTime != 0)
            {
                return HandleBlockingRead(blockingTime, keys, starts);
            }

            int startsIndex = 0;
            
            foreach (var key in keys)
            {
                result = GetXReadOutputs(keys, starts, streamStore);
            }

            return Utilities.BuildXReadOutputArrayString(result.ToArray());
        }

        private static string HandleBlockingRead(int blockingTime, string[] keys, string[] starts)
        {
            Console.WriteLine("Blocking read for: " + blockingTime);
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            List<XReadOutput> result = [];
            isBlockingRead = true;
            while (stopwatch.ElapsedMilliseconds < blockingTime)
            {
                if (newlyAddedStreamStore.Count != 0)
                {
                    result = GetXReadOutputs(keys, starts, newlyAddedStreamStore);

                    if (result.Count != 0)
                    {
                        newlyAddedStreamStore.Clear();
                        return Utilities.BuildXReadOutputArrayString(result.ToArray());
                    }
                }
            }
            isBlockingRead = false;
            newlyAddedStreamStore.Clear();
            // If the timer runs out, return the null output
            return "$-1\r\n";
        }

        private static List<XReadOutput> GetXReadOutputs(string[] keys, string[] starts, Dictionary<string, StreamEntry> store)
        {
            List<XReadOutput> result = [];
            int startsIndex = 0;
            foreach (var key in keys)
            {
                if (store.TryGetValue(key, out StreamEntry? streamEntry))
                {
                    string[] startParts = starts[startsIndex++].Split("-");
                    var streamEntries = streamEntry.Store.Where(kvp =>
                    {
                        string streamEntryId = kvp.Key;
                        string[] streamEntryIdParts = streamEntryId.Split("-");
                        int streamEntryIdTime = int.Parse(streamEntryIdParts[0]);
                        int streamEntryIdSeq = int.Parse(streamEntryIdParts[1]);
                        return streamEntryIdTime >= int.Parse(startParts[0]) && streamEntryIdSeq >= int.Parse(startParts[1]);
                    }).ToArray();
                    List<XRangeOutput> outputs = [];
                    foreach (var streamEntryRes in streamEntries)
                    {
                        string streamEntryKey = streamEntryRes.Key;
                        string[] valueParts = streamEntryRes.Value.Select(kvp => new string[] { kvp.Key, kvp.Value }).SelectMany(x => x).ToArray();
                        outputs.Add(new XRangeOutput { Id = streamEntryKey, Fields = valueParts });
                    }
                    result.Add(new XReadOutput { StreamName = key, Outputs = outputs });
                }
            }
            return result;
        }

        private static string HandleXRANGE(string[] input)
        {
            if (input.Length != 9)
            {
                throw new Exception("Invalid input for handle xrange.");
            }
            string key = input[4];
            
            string start = input[6];
            string[] startParts = start.Split("-");
            string startTimestamp = startParts.Length > 0 ? startParts[0] : string.Empty;
            string startSeq = startParts.Length > 1 ? startParts[1] : string.Empty;

            string end = input[8];
            string[] endParts = end.Split("-");
            string endTimestamp = end == "+" ? string.Empty : endParts[0];
            string endSeq = end == "+" ? string.Empty : endParts[1];

            Console.WriteLine("Start: " + start + " End: " + end);

            if (streamStore.TryGetValue(key, out StreamEntry? streamEntry))
            {
                var streamEntries = streamEntry.Store.Where(kvp =>
                {
                    string streamEntryId = kvp.Key;
                    string[] streamEntryIdParts = streamEntryId.Split("-");
                    int streamEntryIdTime = int.Parse(streamEntryIdParts[0]);
                    int streamEntryIdSeq = int.Parse(streamEntryIdParts[1]);
                    return (startTimestamp == string.Empty || streamEntryIdTime >= int.Parse(startTimestamp)) && 
                           (endTimestamp == string.Empty || streamEntryIdTime <= int.Parse(endTimestamp)) &&
                           (startSeq == string.Empty || streamEntryIdSeq >= int.Parse(startSeq)) && 
                           (endSeq == string.Empty || streamEntryIdSeq <= int.Parse(endSeq));
                }).ToArray();

                XRangeOutput[] result = new XRangeOutput[streamEntries.Length];
                int i = 0;
                foreach (var streamEntryRes in streamEntries)
                {
                    string streamEntryKey = streamEntryRes.Key;
                    string[] valueParts = streamEntryRes.Value.Select(kvp => new string[] { kvp.Key, kvp.Value }).SelectMany(x => x).ToArray();
                    //Console.WriteLine("Stream Entry Key: " + streamEntryKey + " Stream Entry Value: " + streamEntryValue);
                    result[i++] = new XRangeOutput { Id = streamEntryKey, Fields = valueParts };
                }
                return Utilities.BuildNestedArrayString(result);
            }
            else
            {
                return "*0\r\n";
            }
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
