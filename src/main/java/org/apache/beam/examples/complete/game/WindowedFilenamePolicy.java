package org.apache.beam.examples.complete.game;

import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

public class WindowedFilenamePolicy extends FilenamePolicy {
    private final ResourceId prefix;

    public WindowedFilenamePolicy(ResourceId prefix) {
      this.prefix = prefix;
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Unsupported.");
    }

	@Override
    public ResourceId windowedFilename(
    		int shardNumber,
            int numShards,
            BoundedWindow window,
            PaneInfo paneInfo,
            OutputFileHints outputFileHints) {
		IntervalWindow intervalWindow = (IntervalWindow) window;
		System.out.println("getMillis_test:" + window.TIMESTAMP_MAX_VALUE.getMillis());
		String filename = String.format(
            "%s-%s-%s-of-%s.avro",
            "TrackingAd",
            intervalWindow.end().toDateTime(DateTimeZone.forID("Asia/Tokyo")).toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")),
            shardNumber,
            numShards
        );
		System.out.println("filename:" + filename);
        String subDirectory = intervalWindow.end().toDateTime(DateTimeZone.forID("Asia/Tokyo")).toString(DateTimeFormat.forPattern("yyyy/MM/dd/HH"));
        return prefix.getCurrentDirectory()
                .resolve(subDirectory, ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
        		.resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
	}

}
