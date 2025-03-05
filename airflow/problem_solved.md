1. Unable to connect port  
    a. delete vm port then connect  
    b. found more than 1 same airflow images, deleted useless one  

2. scheduler can't work
    run airflow scheduler start

3. unable to use spark solved: add package in requirement -> ensure docker-compose have dockerfile, and right loction
4. to utilise spark: solved: airflow version and python version, forgot to add JAVE_HOME  

Need to do  
1. split green and yellow to 2 tables
2. create delta (change-based) extraction in airflow 