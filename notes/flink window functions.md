
## Window types


1. Tumbling Window
    1. fixed size window
    2. no overlapping time
    3. for example 1-2 seconds 2-4 seconds 4-6 seconds 6-8 seconds is a time window of size 2
2. Sliding Window
    1. fixed size window
    2. overlapping times
    3. 2 parameters
    4. for example 1-3 seconds 2-5 seconds 3-6 seconds 4-9 seconds is a time window of size 3 with a sliding time of 1 second
3. Session Window
    1. not fixed window size
    2. capture how many events happen with at most X amount of time distance apart
    3. i.e how many events happened no more than 1 second apart
        1. event 1 - 1 second event 2 - 2 second event 3 - 4 second => 2 groups
            1. group 1 - event 1 and 2
            2. group 2 - event 3
4. Global Window
    1. not time based
    2. fixed amount of events
    3. group every X amount of events
    4. i.e. if group size = 2
        1. i.e event 1 - 2s event 2 - 3s event 3 - 4s event 4 - 5s
            1. group 1 = event 1, event 2
            2. group 2 = event 4, event 5



## Keyed streams
* method to split streams into mini streams by key
* this ensures all events with same key will be processed by same task.
* We can apply window functions for keyed streams too and this window function takes in a key into its apply method



## Watermarking

Issues happen when event time does not match process time. I.e there is delay and so it gets processed later. In this case we need to say how long extra should we keep our window open to wait for delayed events.
* i.e
    * watermark = 2 seconds
    * tumbling window of size 3 seconds
    * event 1 = event time 1s, process time 1s; event 2 = 2 second event time, process time 2 second; event 3 = 2 seconds but gets processed 5 seconds later
    * we expect event 1 2 and 3 to be in same group given they belong in same 3 seconds but given event 3 was processed at 5 seconds and last event was event 2 at 2 seconds and watermark is 2 seconds then we needed event 3 to be processed at 3 seconds.
    * expected result: group = event1, event2, event3
    * actual result: group = event1, event2 (we dropped event 3)
