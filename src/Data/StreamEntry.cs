namespace codecrafters_redis.src.Data
{
    class StreamEntry
    {
        public Dictionary<string, Dictionary<string, string>> Store { get; set; } = [];

        public void AddToStore(string key, string streamKey, string streamValue)
        {
            if (Store.TryGetValue(key, out Dictionary<string, string>? value))
            {
                value.Add(streamKey, streamValue);
            }
            else
            {
                Store.Add(key, new Dictionary<string, string> { { streamKey, streamValue } });
            }
        }
    }
}
