package dev.philcantcode.worldspecimen;

import android.app.Activity;
import android.os.Build;
import android.os.Bundle;
import android.widget.TextView;

import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

public final class MainActivity extends Activity {
    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        String mode = getIntent().getStringExtra("mode");
        if ("crash".equals(mode)) {
            throw new IllegalStateException("requested world specimen crash");
        }
        try {
            JSONObject report = new JSONObject();
            report.put("package", getPackageName());
            report.put("sdk", Build.VERSION.SDK_INT);
            report.put("mode", mode == null ? "normal" : mode);
            report.put("files_dir", getFilesDir().getCanonicalPath());
            report.put("host_docker_socket_visible", new File("/var/run/docker.sock").exists());
            report.put("host_workspace_visible", new File("/workspace").exists());
            byte[] bytes = report.toString().getBytes(StandardCharsets.UTF_8);
            File destination = new File(getFilesDir(), "world-report.json");
            try (FileOutputStream output = new FileOutputStream(destination, false)) {
                output.write(bytes);
                output.getFD().sync();
            }
            TextView view = new TextView(this);
            view.setText("WORLD_SPECIMEN_READY " + bytes.length);
            view.setTextSize(20);
            setContentView(view);
        } catch (Exception error) {
            throw new RuntimeException("world specimen report failed", error);
        }
    }
}
