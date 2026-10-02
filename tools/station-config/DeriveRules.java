import com.example.channel.importer.PlcProject;
import com.example.channel.importer.PlcProjectParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * writable-rules.tsv из проверенной модели данных стенда (SCADA-gateway/.../controllers.yaml, где
 * writable расставил tools/build_data_model.py по правилу «команды и уставки — да, показания —
 * нет»). Правило — пара (вид объекта, поле); противоречия внутри пары печатаются и в таблицу не
 * идут (такое поле генератор оставит только на чтение).
 *
 * <p>args: controllers.yaml стенда, папка проекта ПЛК, выходной tsv.
 */
public class DeriveRules {

    private static final Pattern TAG = Pattern.compile(
            "deviceName: \"([^\"]+)\", fieldName: \"([^\"]+)\".*?writable: (true|false)");

    public static void main(String[] args) throws IOException {
        PlcProject plc = PlcProjectParser.parse(read(Path.of(args[1], "main.io.lua")),
                read(Path.of(args[1], "main.objects.lua")));
        Map<String, int[]> votes = new TreeMap<>(); // ключ → [RO, RW]
        for (String line : Files.readAllLines(Path.of(args[0]))) {
            Matcher m = TAG.matcher(line);
            if (!m.find()) continue;
            String device = m.group(1), field = m.group(2);
            // В модели стенда у части параметров станции прибором записан сам параметр
            // (deviceName = fieldName = P_V_OFF_DELAY_TIME) — в ПЛК это поле t.SYSTEM.
            String kind = device.equals(field) ? "SYSTEM" : StationConfig.kind(device, plc);
            String fieldKey = StationConfig.Rules.fieldKey(field);
            boolean rw = Boolean.parseBoolean(m.group(3));
            // Уставка прибора (P_* у V, M, TE, LT…) — запись по правилу модели; в модели стенда
            // часть их осталась RO из-за выключенных линий и приборов без архива.
            if (fieldKey.startsWith("P_") && kind.matches("[A-Z]{1,3}")) rw = true;
            votes.computeIfAbsent(kind + "\t" + fieldKey, k -> new int[2])[rw ? 1 : 0]++;
        }
        StringBuilder tsv = new StringBuilder("""
                # Право записи канала реальной станции: вид объекта, поле, RW|RO.
                # Выведено tools/station-config/DeriveRules.java из модели данных стенда
                # (tools/build_data_model.py: запись — только команды и уставки; уставки приборов P_* —
                # всегда RW, параметры станции — поля SYSTEM). Поле массива — без
                # индекса, кроме RT_PAR_F линии (там индекс — разные параметры). Поля нет в таблице —
                # канал только на чтение. Править можно руками: генератор читает этот файл.
                # вид\tполе\tдоступ
                """);
        int conflicts = 0;
        for (var e : votes.entrySet()) {
            int ro = e.getValue()[0], rw = e.getValue()[1];
            if (ro > 0 && rw > 0) {
                conflicts++;
                System.out.println("противоречие (в таблицу не идёт): " + e.getKey() + "  RO×" + ro + " RW×" + rw);
                continue;
            }
            tsv.append(e.getKey()).append('\t').append(rw > 0 ? "RW" : "RO").append('\n');
        }
        Files.writeString(Path.of(args[2]), tsv);
        System.out.println("Правил: " + (votes.size() - conflicts) + ", противоречий: " + conflicts + " → " + args[2]);
    }

    private static byte[] read(Path p) throws IOException {
        return Files.exists(p) ? Files.readAllBytes(p) : new byte[0];
    }
}
