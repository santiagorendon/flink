# Multiple Streams

We can join streams in several ways

* union join
  * join two streams by a key
* window join
  * join two streams by a key and window
  * i.e for every 1 hour how many times did user purchase product (stream 1) and see a product (stream 2)
* interval join
  * this happens on event time not process time
  * join two streams by a key and if the two events were X amount of time away from each other
  * i.e trying to join customer purchases to ads that were clicked in last 14 days
    * the purchase and click can be at most 14 days apart
* connected streams
  * connect two streams and have them be run by same function
  * i.e keeping live ratio between two distinct event types i.e page visits vs purchases realtime ratio