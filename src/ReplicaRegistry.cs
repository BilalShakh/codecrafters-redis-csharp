using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis.src
{
    class ReplicaRegistry
    {
        private static readonly Dictionary<int, Socket> _replicaRegistry = [];
        private static readonly Dictionary<int, int> commandsFinished = [];

        public static void RegisterReplica(int key, Socket value)
        {
            _replicaRegistry[key] = value;
        }
        
        public static Socket GetReplica(int key)
        {
            return _replicaRegistry[key];
        }

        public static bool IsReplicaFinished(int key, int bytesRequested)
        {
            string replConfRequest = Utilities.BuildArrayString(["REPLCONF", "getack", "*"]);
            byte[] replConfData = Encoding.ASCII.GetBytes(replConfRequest);
            _replicaRegistry[key].Send(replConfData);

            byte[] buffer = new byte[1024];
            int bytesRead = _replicaRegistry[key].Receive(buffer);
            string response = Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim();

            var requests = ParseRESP(response);
            if (requests.Count > 0 && requests[0].Length >= 3 && requests[0][1] == "ACK")
            {
                if (int.TryParse(requests[0][2], out int bytesProcessed))
                {
                    return bytesProcessed >= bytesRequested;
                }
            }
            return false;
        }

        static List<string[]> ParseRESP(string data)
        {
            var lines = data.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var result = new List<string[]>();
            var currentArray = new List<string>();
            int expectedLength = 0;
            bool inArray = false;
            var validDataStarted = false;

            for (int i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith("*") || lines[i].StartsWith("$"))
                {
                    validDataStarted = true;
                }

                if (!validDataStarted)
                {
                    continue;
                }

                if (lines[i].StartsWith("*"))
                {
                    if (currentArray.Count > 0)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                    }
                    expectedLength = int.Parse(lines[i].Substring(1)) * 2;
                    inArray = true;
                }
                else if (lines[i].StartsWith("$"))
                {
                    int length = int.Parse(lines[i].Substring(1));
                    if (i + 1 < lines.Length && lines[i + 1].Length == length)
                    {
                        currentArray.Add(lines[i + 1]);
                        i++; // Skip the next line as it is part of the bulk string
                    }
                    if (inArray && currentArray.Count == expectedLength / 2)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                        inArray = false;
                    }
                }
                else
                {
                    if (currentArray.Count > 0)
                    {
                        result.Add(currentArray.ToArray());
                        currentArray.Clear();
                    }
                    currentArray.Add(lines[i]);
                    result.Add(currentArray.ToArray());
                    currentArray.Clear();
                }
            }

            if (currentArray.Count > 0)
            {
                result.Add(currentArray.ToArray());
            }

            return result;
        }

        public static int GetReplicasFinishedCount(int bytesRequested)
        {
            int result = 0;

            foreach (var replica in _replicaRegistry)
            {
                if (IsReplicaFinished(replica.Key, bytesRequested))
                {
                    result++;
                }
            }
            return _replicaRegistry.Count;
        }
    }
}
