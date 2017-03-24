# spring-sparkpi
A Java implementation of SparkPi using Spring Boot

This application is an example tutorial for the
[radanalytics.io](http://radanalytics.io) community. It is intended to be
used as a source-to-image application.

## Quick start

You should have access to an OpenShift cluster and be logged in with the
`oc` command line tool.

1. Create the necessary infrastructure objects
   ```bash
   oc create -f http://radanalytics.io/resources.yaml
   ```

1. Launch spring-sparkpi
   ```bash
   oc new-app --template oshinko-java-spark-build-dc \
       -p APPLICATION_NAME=spring-sparkpi \
       -p GIT_URI=https://github.com/radanalyticsio/spring-sparkpi \
       -p APP_FILE=SparkPiBoot-0.0.1-SNAPSHOT.jar
   ```

1. Expose an external route
   ```bash
   oc expose svc/spring-sparkpi
   ```

1. Visit the exposed URL with your browser or other HTTP tool, for example:
   ```bash
   $ curl http://`oc get routes | grep spring-sparkpi | awk '{ print $2 }'`
   Pi is rouuuughly 3.1335
   ```
