package com.esri.geoevent.processor.delay;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class DelayProcessorService extends GeoEventProcessorServiceBase
{
  private Messaging messaging;

  public DelayProcessorService()
  {
    this.definition = new DelayProcessorDefinition();
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    DelayProcessor delayProcessor = new DelayProcessor(definition);
    delayProcessor.setMessaging(messaging);
    return delayProcessor;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }
}
