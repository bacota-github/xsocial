The object is to identify all DeviceLocations that are close (within 50 meters) to some "point of interest".  The points of interest are represented as circles with a center and a radius.

There are two different solutions here

* xsocialRDD.scala -- A basic rdd solution.  To avoid comparing every DeviceLocation to every point of interest, the world is divided into  a grid and DeviceLocations are only compared to points of interest that are within range of the the DeviceLocation.

* xsocialSql.scala -- A spark SQL solution that joins the DeviceLocations to Points of Interest using a where clause that should filter most (DeviceLocation, PointOfInterest) that are too far apart.  

* xsocial.scala -- Code shared between the two solutions.

* app.scala -- A class with a main method that runs spark locally using the Spark SQL implementation

* testXSocial.scala -- A simple unit test.
