Embulk::JavaPlugin.register_filter(
  "nashorn_test", "org.embulk.filter.nashorn_test.NashornTestFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
