package com.esri.geoevent.processor.delay;

import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DelayProcessorDefinition extends GeoEventProcessorDefinitionBase
{
    public static final String DELAY_VALUE = "delayValue";
    public static final String DELAY_VALUE_UNITS = "delayValueUnits";

    public DelayProcessorDefinition() {

        final List<LabeledValue> allowedTimeUnits = new ArrayList<>();
        for(TimeUnit value: TimeUnit.values()) {
            allowedTimeUnits.add(new LabeledValue(value.name(), value.name()));
        }

        try {
            propertyDefinitions.put(DELAY_VALUE,
                new PropertyDefinition(DELAY_VALUE,
                    PropertyType.Long,
                    "0",
                    "Delay Time",
                    "Specify a delay value. GeoEvents will be delayed by this value before they are further processed.",
                    true,
                    false));

            propertyDefinitions.put(DELAY_VALUE_UNITS,
                new PropertyDefinition(DELAY_VALUE_UNITS,
                    PropertyType.String,
                    TimeUnit.SECONDS.name(),
                    "Delay Time Unit",
                    "Choose the time unit for the delay value",
                    true,
                    false,
                    allowedTimeUnits));


        } catch (PropertyException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getName()
    {
        return "Delay Processor";
    }

    @Override
    public String getDomain()
    {
        return "com.esri.geoevent.processor";
    }

    @Override
    public String getDescription() {
        return "Delays the processing of events by a specified amount of time. All delayed events are held in a queue in memory.";
    }
}
