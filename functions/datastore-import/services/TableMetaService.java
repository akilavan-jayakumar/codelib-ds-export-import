package services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import pojos.Table;

import java.io.File;
import java.util.List;

public class TableMetaService {
    public static List<Table> getAllTableMetaFromFile(File file) throws Exception {
        String content = DiskFileService.readFileContent(file);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(content, new TypeReference<List<Table>>() {
        });
    }
}
