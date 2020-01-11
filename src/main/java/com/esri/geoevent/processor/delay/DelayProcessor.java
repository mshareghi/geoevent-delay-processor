package com.esri.geoevent.processor.delay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

/**
 * The DelayProcessor class processes incoming GeoEvents and delaying their execution by a specified time. Delayed
 * events are placed in a private queue and processed based on a combination of the delay time and the time they were
 * received by GeoEvent Server (the RECEIVED_TIME property).
 */
public class DelayProcessor extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable
{
  private static final BundleLogger            LOGGER          = BundleLoggerFactory.getLogger(DelayProcessor.class);

  private ExecutorService                      executorService = Executors.newSingleThreadExecutor();
  private final BlockingQueue<DelayedGeoEvent> geoEventQueue   = new DelayQueue<DelayedGeoEvent>();
  private static final int                     MAX_ENTRIES     = 5000;
  private final Map<String, String>            geoEventKeySet  = Collections.synchronizedMap(new LinkedHashMap<String, String>()
                                                                 {
                                                                   private static final long serialVersionUID = 3497816525702669924L;

                                                                   protected boolean removeEldestEntry(Map.Entry<String, String> eldest)
                                                                   {
                                                                     return size() > MAX_ENTRIES;
                                                                   }

                                                                 });

  private Messaging                            messaging;
  private GeoEventProducer                     geoEventProducer;
  private long                                 delayValue;
  private TimeUnit                             delayValueUnit;

  private String                               delayField      = DelayProcessorDefinition.RECEIVED_TIME;
  private boolean                              allowDuplicates = true;
  private boolean                              useTrackID      = false;

  private Future<?>                            eventLoop;

  public DelayProcessor(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
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
    if (geoEvent != null)
    {
      long delayMilliseconds = delayValueUnit.toMillis(delayValue);
      final DelayedGeoEvent delayedGeoEvent = new DelayedGeoEvent(geoEvent, delayMilliseconds, delayField, useTrackID);

      String key = delayedGeoEvent.getKey();
      if (LOGGER.isTraceEnabled())
        LOGGER.trace("Allow Duplicates is {2} Use TRACK_ID is {3} and Getting key {0}: {1}", key, this.geoEventKeySet.get(key), allowDuplicates, useTrackID);
      if (allowDuplicates || (this.geoEventKeySet.put(key, key) == null))
      {
        this.geoEventQueue.put(delayedGeoEvent);
        LOGGER.trace("Added GeoEvent with key {3} to Delay Queue with delay {1} ms and queue size {2}: {0}", geoEvent, delayMilliseconds, this.geoEventQueue.size(), delayedGeoEvent.getKey());
      }
      else
      {
        LOGGER.debug("Discarding duplicate GeoEvent with key {1}: {0}", geoEvent, delayedGeoEvent.getKey());
      }

    }
    return null;
  }

  @Override
  public void onServiceStart()
  {
    super.onServiceStart();

    try
    {
      if (executorService.isShutdown() || executorService.isTerminated())
      {

      }
      if (eventLoop != null)
      {
        this.eventLoop.cancel(true);
        LOGGER.trace("Previous Event Loop Canceled");
      }
      this.eventLoop = executorService.submit(constructEventLoop(this.geoEventQueue));
      LOGGER.trace("Event Loop Started");
    }
    catch (Exception ex)
    {
      LOGGER.error("Failed to start processor.", ex);
    }
  }

  @Override
  public void onServiceStop()
  {
    try
    {
      if (eventLoop != null)
      {
        this.eventLoop.cancel(true);
      }

      this.geoEventQueue.clear(); // TODO: Make this configurable
      LOGGER.debug("Cleared GeoEvent Queue");
    }
    catch (Exception ex)
    {
      LOGGER.error("Failed to stop processor.", ex);
    }
    super.onServiceStop();
  }

  protected Runnable constructEventLoop(final BlockingQueue<DelayedGeoEvent> inboundQueue)
  {
    return () ->
      {
        try
        {
          while (!Thread.currentThread().isInterrupted())
          {
            final DelayedGeoEvent take = inboundQueue.take();
            final GeoEvent geoEvent = take.getGeoEvent();

            if (LOGGER.isTraceEnabled())
            {
              final DelayedGeoEvent peek = inboundQueue.peek();
              LOGGER.trace("Sending Delayed GeoEvent with key {3} from queue with size {1} and time to next event {2} ms: {0}", geoEvent, inboundQueue.size(), peek == null ? 0 : peek.getDelay(TimeUnit.MILLISECONDS), take.getKey());
            }

            try
            {
              send(geoEvent);
            }
            catch (MessagingException e)
            {
              LOGGER.error("Error sending geoevent: {0}", e, geoEvent);
            }
          }
        }
        catch (InterruptedException ie)
        {
          LOGGER.info("Delayed Event consumer loop interrupted.", ie);
        }
      };
  }

  @Override
  public void afterPropertiesSet()
  {
    boolean previousAllowDuplicates = allowDuplicates;
    boolean previousUseTrackID = useTrackID;
    String previousDelayField = delayField;
    try
    {
      this.delayValue = Long.parseLong(this.properties.get(DelayProcessorDefinition.DELAY_VALUE).getValueAsString());

      this.delayValueUnit = TimeUnit.valueOf(this.properties.get(DelayProcessorDefinition.DELAY_VALUE_UNITS).getValueAsString());

      this.delayField = this.properties.get(DelayProcessorDefinition.DELAY_FIELD).getValueAsString();

      this.allowDuplicates = Boolean.parseBoolean(this.properties.get(DelayProcessorDefinition.ALLOW_DUPLICATES).getValueAsString());

      this.useTrackID = Boolean.parseBoolean(this.properties.get(DelayProcessorDefinition.USE_TRACK_ID).getValueAsString());

      if (LOGGER.isTraceEnabled())
      {
        LOGGER.trace("Delay Value: {0} {1}", delayValue, delayValueUnit);
        LOGGER.trace("Delay Field: {0}", delayField);
        LOGGER.trace("Allow Duplicates: {0}", allowDuplicates);
        LOGGER.trace("Use TRACK_ID: {0}", useTrackID);
      }

      if ((previousAllowDuplicates != allowDuplicates) || (previousUseTrackID != useTrackID) || !previousDelayField.equals(delayField))
      {
        // Keys will change, get rid of the old ones
        geoEventKeySet.clear();
        LOGGER.debug("Cleared key cache");
      }
    }
    catch (Exception ex)
    {
      LOGGER.error("Failed to get processor properties.", ex);
    }
    super.afterPropertiesSet();
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException
  {
    if (geoEventProducer != null && geoEvent != null)
    {
      geoEvent.setProperty(GeoEventPropertyName.TYPE, "event");
      geoEvent.setProperty(GeoEventPropertyName.OWNER_ID, getId());
      geoEvent.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
      geoEventProducer.send(geoEvent);
    }
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }

  @Override
  public String getStatusDetails()
  {
    return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
  }

  @Override
  public boolean isGeoEventMutator()
  {
    return true;
  }

  @Override
  public EventDestination getEventDestination()
  {
    return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return (geoEventProducer != null) ? Collections.singletonList(geoEventProducer.getEventDestination()) : new ArrayList<>();
  }

  @Override
  public void disconnect()
  {
    if (geoEventProducer != null)
      geoEventProducer.disconnect();
  }

  @Override
  public boolean isConnected()
  {
    return (geoEventProducer != null) && geoEventProducer.isConnected();
  }

  @Override
  public void setup() throws MessagingException
  {
    ;
  }

  @Override
  public void init() throws MessagingException
  {
    ;
  }

  @Override
  public void update(Observable o, Object arg)
  {
    ;
  }
}
