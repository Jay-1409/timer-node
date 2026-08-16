**06/jul/2026**

a [friend](https://github.com/meet-dharmesh-gandhi) of mine asked me to design a connection pool ( database connection ). Which i did. a followup that i got was to handle the situation in which if a connection given to a particular request, and the request goes into a inf loop how is the connection retrieved back ? 

We discussed the possibility of using a timer based approch in which i designed a min heap based approch where the top of the heap is the timer that will be going off the earliest. 


**07/jul/2026**

I decided to implement the timer as an project. but i wanted to make it a timer node. a node which handles the timer part and can be integrated into any other project or service. 

innitially i was going to implement this in node.js along with express.js but chatgpt convinced me to implement ths in go. 

 - ~ 11:00 : started the project ( no prior exp with go )
 - 18:18 : a working prototype with documentation ready for v1 release

 **16/aug/2026**

I had not benchmarked this project. so i created some benchmarks for this project today. i also found that there is a probably failure case in the system when all heaps are flooded new tasks will get rejected. so i need an mechanism for this. 

some of my options are i think 
- spawn new heaps?
    - maybe spawn new heaps with size increase in power of 2. 
- increase the size of the heap by powers of 2
- i will need to benchmark either of these possibilites 

