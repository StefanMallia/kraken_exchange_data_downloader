
pub enum KrakenMessage
{
    TradeMessage
    {
        price: String,
        volume: String,
        timestamp: String,
        local_time: String,
        side: String,
        order_type: String,
        misc: String,
        index_in_message: String
    },
    DepthMessage
    {
        price: String,
        volume: String,
        is_republish: String,
        timestamp: String,
        local_time: String,
        index_in_message: String,
        is_ask: String
    },
    SpreadMessage
    {
        bid: String,
        ask: String,
        bid_volume: String,
        ask_volume: String,
        timestamp: String,
        local_time: String
    },
    OrderBookMessage
    {
        price: String,
        volume: String,
        timestamp: String,
        local_time: String,
        index_in_message: String,
        is_ask: String
    }
}
