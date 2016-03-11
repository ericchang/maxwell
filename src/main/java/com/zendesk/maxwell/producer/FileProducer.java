package com.zendesk.maxwell.producer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.EnumSet;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

public class FileProducer extends AbstractProducer {
	private final File file;
	private final FileWriter fileWriter;

	public FileProducer(MaxwellContext context, String filename, Format format) throws IOException {
		super(context, EnumSet.of(Format.JSON, Format.AVRO_JSON), format);
		this.file = new File(filename);
		this.fileWriter = new FileWriter(this.file, true);
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.fileWriter.write(new String(serialize(r, null), "UTF-8"));
		this.fileWriter.write('\n');
		this.fileWriter.flush();

		context.setPosition(r);
	}
}
