namespace codecrafters_redis.src.Data
{
    public class XReadOutput
    {
        public required string StreamName { get; set; }
        public required List<XRangeOutput> Outputs { get; set; }
    }
}
