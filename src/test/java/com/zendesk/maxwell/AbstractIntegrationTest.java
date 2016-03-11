package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

public class AbstractIntegrationTest extends AbstractMaxwellTest {
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};
	private static final String EXPECTED_JSON_LINE = "^\\s*\\->.*";

	ObjectMapper mapper = new ObjectMapper();
	protected Map<String, Object> parseJSON(String json) throws Exception {
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		return mapper.readValue(json, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF);
	}

	private void runJSONTest(List<String> sql, List<Map<String, Object>> expectedJSON) throws Exception {
		List<Map<String, Object>> eventJSON = new ArrayList<>();
		List<Map<String, Object>> matched = new ArrayList<>();
		List<RowMap> rows = getRowsForSQL(null, sql.toArray(new String[sql.size()]));

		for ( RowMap r : rows ) {
			String s = rowToJson(r);

			Map<String, Object> outputMap = sanitizeMap(parseJSON(s));

			eventJSON.add(outputMap);

			for ( Map<String, Object> b : expectedJSON ) {
				if ( outputMap.equals(sanitizeMap(b)) )
					matched.add(b);
			}
		}

		for ( Map j : matched ) {
			expectedJSON.remove(j);
		}

		if ( expectedJSON.size() > 0 ) {
			String msg = "Did not find: \n" +
						 StringUtils.join(expectedJSON.iterator(), "\n") +
						 "\n\n in : " +
						 StringUtils.join(eventJSON.iterator(), "\n");
			assertThat(msg, false, is(true));

		}
	}

	protected Map<String, Object> sanitizeMap(Map<String, Object> jsonMap) throws Exception {
		Map<String, Object> sanitized = new LinkedHashMap<>(jsonMap);
		sanitized.remove("ts");
		sanitized.remove("xid");
		sanitized.remove("commit");
		return sanitized;
	}

	protected String rowToJson(RowMap rowMap) throws Exception {
		return rowMap.toJSON();
	}

	/** See https://github.com/spullara/mustache.java */
	protected void runJSONMustacheTemplateFile(String fname, Map<String, Object> mustacheScopes) throws Exception {
		Writer writer = new StringWriter();
		MustacheFactory mf = new DefaultMustacheFactory();
		Mustache mustache = mf.compile(new BufferedReader(new FileReader(new File(fname))), "test");
		mustache.execute(writer, mustacheScopes);

		runJSONTest(new BufferedReader(new StringReader(writer.toString())));
	}

	protected void runJSONTestFile(String fname) throws Exception {
		File file = new File(fname);
		BufferedReader reader = new BufferedReader(new FileReader(file));

		runJSONTest(reader);
	}

	protected void runJSONTest(BufferedReader reader) throws Exception {
		ArrayList<Map<String, Object>> jsonAsserts = new ArrayList<>();
		ArrayList<String> inputSQL = new ArrayList<>();

		ObjectMapper mapper = new ObjectMapper();

		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		StringBuilder buf = new StringBuilder();
		for ( String line = reader.readLine(); line != null; line = reader.readLine() ) {
			if ( skipLine(line) ) {
				flushBuffer(jsonAsserts, mapper, buf);
				continue;
			}

			if ( line.matches(EXPECTED_JSON_LINE) ) {
				// support multi-line expected json strings
				buf.append(line.replaceAll("^\\s*\\->\\s*", ""));
			} else {
				flushBuffer(jsonAsserts, mapper, buf);
				inputSQL.add(line);
			}
		}
		reader.close();

		flushBuffer(jsonAsserts, mapper, buf);

	    runJSONTest(inputSQL, jsonAsserts);
	}

	private void flushBuffer(ArrayList<Map<String, Object>> jsonAsserts, ObjectMapper mapper,
							 StringBuilder buf) throws IOException {
		if ( buf.length() > 0 ) {
			String json = buf.toString();
			jsonAsserts.add(mapper.<Map<String, Object>>readValue(json, MaxwellIntegrationTest.MAP_STRING_OBJECT_REF));
			buf.setLength(0);
		}
	}

	private boolean skipLine(String line) {
		return line.matches("^\\s*$") || line.matches("^\\s*//.*$");
	}
}
