#Custom JMeter sampler for Mongodb

This is a custom java sampler class that can be used to benchmark Mongo.
It was tested against MongoDB 2.4.6

Version 0.1 (alpha) 
 
Written by: Alex Bordei Bigstep
(alex at bigstep dt com)

##Dependencies:
* apache jmeter sources 2.11 
* mongo java client 2.12

##How to use
Copy the .java file over inside the sources. 
You will need to copy the mongo jar over ./lib/opt and ./lib. You need to copy them in both locations. For some reason the compilation works but the jars from/opt do not get distributed.

* mongo-java-driver-2.12.3.jar

```
ant package-only
```
Run jmeter as ususual from the newly created bin file. 
```
sh ./bin/jmeter.sh 
```

Add a new jmeter Java sampler, use the com.bigstep.MongoSampler class.
![Alt text](/img/jmeter1.png?raw=true "Select jmeter custom sampler")

Configure your credentials and everything
![Alt text](/img/jmeter2.png?raw=true "Configure jmeter sampler")


