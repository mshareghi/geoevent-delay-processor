package com.esri.geoevent.processor.delay;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import com.esri.ges.core.geoevent.GeoEvent;

public class DelayedGeoEvent implements Delayed
{
  private final GeoEvent geoEvent;
  private final long     time;
  private final boolean  useTrackID;
  private final String   key;

  public DelayedGeoEvent(GeoEvent geoEvent, long delayTimeMs, String delayTimeField, boolean useTrackID)
  {
    this.geoEvent = geoEvent;
    this.useTrackID = useTrackID;

    if (DelayProcessorDefinition.TIME_END.equals(delayTimeField))
      this.time = geoEvent.getEndTime().getTime() + delayTimeMs;
    else if (DelayProcessorDefinition.TIME_START.equals(delayTimeField))
      this.time = geoEvent.getStartTime().getTime() + delayTimeMs;
    else
      this.time = geoEvent.getReceivedTime().getTime() + delayTimeMs;

    if (useTrackID)
    {
      // key must start with time to preserve queue time order
      this.key = this.time + "_" + geoEvent.getTrackId();
    }
    else
    {
      this.key = Long.toString(time);
    }
  }

  public String getKey()
  {
    return this.key;
  }

  @Override
  public long getDelay(TimeUnit unit)
  {
    long diff = this.time - System.currentTimeMillis();
    return unit.convert(diff, TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed obj)
  {
    int result = Long.compare(this.time, ((DelayedGeoEvent) obj).time);
    if (useTrackID)
    {
      result = this.key.compareTo(((DelayedGeoEvent) obj).key);
    }

    return result;
  }

  public GeoEvent getGeoEvent()
  {
    return this.geoEvent;
  }
}
