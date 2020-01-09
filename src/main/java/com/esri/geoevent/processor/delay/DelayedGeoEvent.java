package com.esri.geoevent.processor.delay;

import com.esri.ges.core.geoevent.GeoEvent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedGeoEvent implements Delayed {
    private final GeoEvent geoEvent;
    private final long time;

    public DelayedGeoEvent(GeoEvent geoEvent, long delayTimeMs) {
        this.geoEvent = geoEvent;
        this.time = geoEvent.getReceivedTime().getTime() + delayTimeMs;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = this.time - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed obj)
    {
        return Long.compare(this.time, ((DelayedGeoEvent)obj).time);
    }

    public GeoEvent getGeoEvent() {
        return this.geoEvent;
    }
}
