# GeoEvent Delay Processor

This custom processor provides the capability to delay execution of one or more events based on a configurable 
delay interval and unit. Incoming GeoEvents are added to a queue and held in memory until their intervals have 
elapsed, after which they are processed asynchronously in the GeoEvent service.

This type of processor can be useful where administrators want to support streaming location data to users but 
do not want to support near real-time updates.

![Example](geoevent-delay-processor.png?raw=true)

## Features
* GeoEvent Delay Processor

## Instructions

Building the source code:

1. Make sure Maven and ArcGIS GeoEvent Server SDK are installed on your machine.
2. Run 'mvn install -Dcontact.address=[YourContactEmailAddress]'

Installing the built jar files:

1. Copy the *.jar files under the 'target' sub-folder(s) into the [ArcGIS-GeoEvent-Server-Install-Directory]/deploy folder.

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Support

This component is not officially supported as an Esri product. The source code is available under the Apache License. 

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Usage

* The following parameters are supported:
  * `Delay Value` Specifies a constant value by which GeoEvents will be delayed before further processing.
   Delayed events are added to queue and held in memory.
  * `Delay Value Unit` specifies the time unit for the delay value.


