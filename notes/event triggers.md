Determine when to trigger window functions
* can run multiple times on same window
* has nothing do with the window, i.e event grouping

event time trigger - happens by default when the watermark is > window end time (automatic for event time windows)
processing time trigger - fires when the current system time > window end time (automatic for processing time windows)
types of different triggers user can manually define:
* for example trigger every X elements
* or trigger every X time
* purging triggers
    * clear existing events after X
* custom triggers - out of scope for lesson