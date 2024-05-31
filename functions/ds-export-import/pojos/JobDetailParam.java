package pojos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobDetailParam {
   private List<Table> tables =  new ArrayList<>();
   private List<SubJob> subJobs = new ArrayList<>();
   private Map<String,String> files =  new HashMap<>();

   public List<Table> getTables() {
      return tables;
   }

   public void setTables(List<Table> tables) {
      this.tables = tables;
   }

   public List<SubJob> getSubJobs() {
      return subJobs;
   }

   public void setSubJobs(List<SubJob> subJobs) {
      this.subJobs = subJobs;
   }

   public Map<String, String> getFiles() {
      return files;
   }

   public void setFiles(Map<String, String> files) {
      this.files = files;
   }
}
