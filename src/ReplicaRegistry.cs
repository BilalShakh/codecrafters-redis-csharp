using System.Net.Sockets;

namespace codecrafters_redis.src
{
    class ReplicaRegistry
    {
        public static readonly Dictionary<int, Socket> Replicas = [];
        public static readonly Dictionary<int, bool> ReplicasFinished = [];

        public static void RegisterReplica(int key, Socket value)
        {
            Replicas[key] = value;
            ReplicasFinished[key] = true;
        }
    }
}
