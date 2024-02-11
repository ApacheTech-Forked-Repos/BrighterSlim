namespace ApacheTech.Common.BrighterSlim
{
    public interface ICall : IRequest
    {
        /// <summary>
        /// The address of the queue to reply to - usually private to the sender
        /// </summary>
        ReplyAddress ReplyAddress { get; }
    }
}
