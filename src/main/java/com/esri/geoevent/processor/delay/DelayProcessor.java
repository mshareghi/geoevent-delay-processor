package com.esri.geoevent.processor.delay;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.*;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Observable;
import java.util.concurrent.*;

/**
 * The DelayProcessor class processes incoming GeoEvents and delaying their execution by a specified time. Delayed
 * events are placed in a private queue and processed based on a combination of the delay time and the time they were
 * received by GeoEvent Server (the RECEIVED_TIME property).
 */
public class DelayProcessor extends GeoEventProcessorBase
    implements GeoEventProducer, EventUpdatable
{
    private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(DelayProcessor.class);

    private final ExecutorService executorService =  Executors.newSingleThreadExecutor();
    private final BlockingQueue<DelayedGeoEvent> geoEventQueue = new DelayQueue<>();

    private Messaging messaging;
    private GeoEventProducer geoEventProducer;
    private long delayValue;
    private TimeUnit delayValueUnit;
    private Future<?> eventLoop;

    public DelayProcessor(GeoEventProcessorDefinition definition) throws ComponentException {
        super(definition);
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
        super.shutdown();
    }

    @Override
    public void setId(String id)
    {
        super.setId(id);
        geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
    }


    @Override
    public GeoEvent process(GeoEvent geoEvent) throws Exception
    {
        if(geoEvent != null)
        {
            long delayMilliseconds = delayValueUnit.toMillis(delayValue);
            final DelayedGeoEvent delayedGeoEvent = new DelayedGeoEvent(geoEvent, delayMilliseconds);

            geoEventQueue.put(delayedGeoEvent);

            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                    "Added GeoEvent to Delay Queue: %s | delay: %d ms | queue size: %d",
                    geoEvent.getGuid(),
                    delayMilliseconds,
                    this.geoEventQueue.size()));
            }

        }
        return null;
    }


    @Override
    public void onServiceStart() {
        super.onServiceStart();

        try {
            if (eventLoop != null) {
                this.eventLoop.cancel(true);
            }
            this.eventLoop = executorService.submit(constructEventLoop(this.geoEventQueue));
        }
        catch(Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    @Override
    public void onServiceStop() {
        try {
            if (eventLoop != null) {
                this.eventLoop.cancel(true);
            }

            this.geoEventQueue.clear(); // TODO: Make this configurable
        }
        catch(Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        super.onServiceStop();
    }

    protected Runnable constructEventLoop(final BlockingQueue<DelayedGeoEvent> inboundQueue) {
        return () -> {
            try {
                while (!Thread.currentThread().isInterrupted())
                {
                    final GeoEvent geoEvent = inboundQueue.take().getGeoEvent();

                    if(LOGGER.isDebugEnabled()) {
                        final DelayedGeoEvent peek = this.geoEventQueue.peek();
                        LOGGER.debug(String.format(
                            "Processing Delayed GeoEvent: %s | queue size: %d | time to next event: %d ms",
                            geoEvent.getGuid(),
                            this.geoEventQueue.size(),
                            peek == null ? 0 : peek.getDelay(TimeUnit.MILLISECONDS)));
                    }

                    try {
                        send(geoEvent);
                    } catch (MessagingException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
            catch(InterruptedException ie) {
                LOGGER.info("Thread interrupted:" + ie.getMessage());
            }
        };
    }


    @Override
    public void afterPropertiesSet() {
        try {
            this.delayValue = Long.parseLong(
                this.properties.get(DelayProcessorDefinition.DELAY_VALUE).getValueAsString());

            this.delayValueUnit = TimeUnit.valueOf(
                this.properties.get(DelayProcessorDefinition.DELAY_VALUE_UNITS).getValueAsString());
        }
        catch(Exception ex) {
            LOGGER.error(ex.getMessage());
        }
        super.afterPropertiesSet();
    }

    @Override
    public void send(GeoEvent geoEvent) throws MessagingException {
        if (geoEventProducer != null && geoEvent != null)
        {
            geoEvent.setProperty(GeoEventPropertyName.TYPE, "event");
            geoEvent.setProperty(GeoEventPropertyName.OWNER_ID, getId());
            geoEvent.setProperty(GeoEventPropertyName.OWNER_URI,definition.getUri());
            geoEventProducer.send(geoEvent);
        }
    }

    public void setMessaging(Messaging messaging)
    {
        this.messaging = messaging;
    }

    @Override
    public String getStatusDetails() {
        return (geoEventProducer != null)
            ? geoEventProducer.getStatusDetails()
            : "";
    }

    @Override
    public boolean isGeoEventMutator() {
        return true;
    }

    @Override
    public EventDestination getEventDestination() {
        return (geoEventProducer != null)
            ? geoEventProducer.getEventDestination()
            : null;
    }

    @Override
    public List<EventDestination> getEventDestinations() {
        return (geoEventProducer != null)
            ? Collections.singletonList(geoEventProducer.getEventDestination())
            : new ArrayList<>();
    }

    @Override
    public void disconnect() {
        if (geoEventProducer != null)
            geoEventProducer.disconnect();
    }

    @Override
    public boolean isConnected() {
        return (geoEventProducer != null) && geoEventProducer.isConnected();
    }

    @Override
    public void setup() throws MessagingException {
        ;
    }

    @Override
    public void init() throws MessagingException {
        ;
    }

    @Override
    public void update(Observable o, Object arg) {
        ;
    }
}
