package org.embulk.filter.nashorn_test;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;


public class NashornTestFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("column")
        public String getColumn();

        // configuration option 2 (optional string, null is not allowed)
        @Config("script")
        public String getScript();

    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final String script = task.getScript();
        final String column_name = task.getColumn();


        return new PageOutput() {
            private final PageReader reader = new PageReader(inputSchema);
            private final PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("JavaScript");

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            @Override
            public void add(Page page)
            {
                reader.setPage(page);
                while (reader.nextRecord()) {

                    for (Column column : outputSchema.getColumns()) {
                        if (column.getName().equals(column_name)) {


                            engine.put("value",reader.getLong(column));
                            try {
                                Double ret;
                                ret = (Double)engine.eval(script);
                                builder.setLong(column, ret.longValue());
                            } catch (ScriptException e) {
                                e.printStackTrace();
                            }

                            continue;
                        }
                        if (reader.isNull(column)) {
                            builder.setNull(column);
                            continue;
                        }
                        if (Types.STRING.equals(column.getType())) {
                            builder.setString(column, reader.getString(column));
                        }
                        else if (Types.BOOLEAN.equals(column.getType())) {
                            builder.setBoolean(column, reader.getBoolean(column));
                        }
                        else if (Types.DOUBLE.equals(column.getType())) {
                            builder.setDouble(column, reader.getDouble(column));
                        }
                        else if (Types.LONG.equals(column.getType())) {
                            builder.setLong(column, reader.getLong(column));
                        }
                        else if (Types.TIMESTAMP.equals(column.getType())) {
                            builder.setTimestamp(column, reader.getTimestamp(column));
                        }
                        else if (Types.JSON.equals(column.getType())) {
                            builder.setJson(column, reader.getJson(column));
                        }
                    }
                    builder.addRecord();
                }
            }
        };


    }
}
