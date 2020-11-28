

- 3 load mixes -> build their trans (max 15) and prob tables
    - see tpcw/rbe/EBTPCW?Factory.java

- 48 sql queries 
    - see sql-mysql.properties
- 14 webpages
    - see tpcw/rbe/RBE.java
- Need to map webpages (each is a transaction) to sqls (multiple queries)

- at each webpage
    - send server HTTP request, and store returned data into client class
    - use data stored in client + trans and prob table to determine which webpage to go next
        - Look at tpcw/rbe/EB.java (specifically run())

- Each transaction must start with BEGIN (even with single query). 
    - Our backend doesn't support query with no BEGIN for now.

- client: 
    - unique ID
    - everything needed by an HTTP request
        - session ID?
        - max session time
        - shopID (shopping cart?)
        - curState
        - nextState

- The generator needs to be able to launch client distributedly:
    - Parameters:
        - think time
        - max session time
        - max number of clients
        - which load mix
