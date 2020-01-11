package com.esri.geoevent.processor.delay;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class DelayProcessorDefinition extends GeoEventProcessorDefinitionBase
{
  private static final BundleLogger LOGGER            = BundleLoggerFactory.getLogger(DelayProcessorDefinition.class);

  public static final String        DELAY_VALUE       = "delayValue";
  public static final String        DELAY_VALUE_UNITS = "delayValueUnits";
  public static final String        DELAY_FIELD       = "delayField";
  public static final String        ALLOW_DUPLICATES  = "allowDuplicates";
  public static final String        USE_TRACK_ID      = "useTrackID";

  public static final String        TIME_START        = "TIME_START";
  public static final String        TIME_END          = "TIME_END";
  public static final String        RECEIVED_TIME     = "RECEIVED_TIME";

  public DelayProcessorDefinition()
  {
    LOGGER.info("Creating {0}", getLabel());
    LOGGER.info("Description {0}", getDescription());

    final List<LabeledValue> allowedTimeUnits = new ArrayList<>();
    for (TimeUnit value : TimeUnit.values())
    {
      allowedTimeUnits.add(new LabeledValue(value.name(), value.name()));
    }

    final List<LabeledValue> allowedTimeFields = new ArrayList<>();
    allowedTimeFields.add(new LabeledValue(TIME_START, TIME_START));
    allowedTimeFields.add(new LabeledValue(TIME_END, TIME_END));
    allowedTimeFields.add(new LabeledValue(RECEIVED_TIME, RECEIVED_TIME));

    try
    {
      propertyDefinitions.put(DELAY_VALUE, new PropertyDefinition(DELAY_VALUE, PropertyType.Long, "0", "Delay Time", "Specify a delay value. GeoEvents will be delayed by this value before they are further processed.", true, false));

      propertyDefinitions.put(DELAY_VALUE_UNITS, new PropertyDefinition(DELAY_VALUE_UNITS, PropertyType.String, TimeUnit.SECONDS.name(), "Delay Time Unit", "Choose the time unit for the delay value", true, false, allowedTimeUnits));

      propertyDefinitions.put(DELAY_FIELD, new PropertyDefinition(DELAY_FIELD, PropertyType.String, RECEIVED_TIME, "Delay Time Field", "Choose the field that the delay time will be added to.", true, false, allowedTimeFields));

      propertyDefinitions.put(ALLOW_DUPLICATES, new PropertyDefinition(ALLOW_DUPLICATES, PropertyType.Boolean, true, "Allow Duplicates?", "Should new events with the same timestamp as an event already in the queue be allowed to be added to the queue? If duplicates are allowed, multiple events with the same timestamp may enter the queue.", true, false));

      propertyDefinitions.put(USE_TRACK_ID, new PropertyDefinition(USE_TRACK_ID, PropertyType.Boolean, false, "Use TRACK_ID?", "Should each unique TRACK_ID get its own queue, or should all events be stored in a single queue.", true, false));
    }
    catch (PropertyException e)
    {
      LOGGER.warn("Failed to construct definition.", e);
    }
  }

  @Override
  public String getName()
  {
    return "DelayProcessor";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.processor";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.processor.delay-processor.PROCESSOR_DESC}";
  }

  @Override
  public String getLabel()
  {
    return "${com.esri.geoevent.processor.delay-processor.PROCESSOR_LABEL}";
  }

}
