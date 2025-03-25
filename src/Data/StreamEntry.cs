namespace codecrafters_redis.src.Data
{
    class StreamEntry
    {
        public string StreamKey { get; set; }
        public Dictionary<string, Dictionary<string, string>> StreamStore { get; set; }
    }
}
