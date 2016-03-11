package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import java.util.EnumSet;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context, Format format) {
		super(context, EnumSet.of(Format.JSON, Format.AVRO_JSON), format);
	}

	@Override
	public void push(RowMap r) throws Exception {
		System.out.println(new String(serialize(r, null), "UTF-8"));
		this.context.setPosition(r);
	}
}
