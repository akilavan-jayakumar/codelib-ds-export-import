package pojos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobDetailParam {
   private List<Table> tables =  new ArrayList<>();
   private List<SubJob> subJobs = new ArrayList<>();
   private Map<String,String> files =  new HashMap<>();
   private List<SelectedTable> selectedTables =  new ArrayList<>();

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

   public List<SelectedTable> getSelectedTables() {
      return selectedTables;
   }

   public void setSelectedTables(List<SelectedTable> selectedTables) {
      this.selectedTables = selectedTables;
   }

   public Map<String,Object> generateResponseMap(){
      Map<String,Object> map =  new HashMap<>();
      map.put("files",files);
      map.put("tables",tables);
      map.put("selected-tables",selectedTables);
      map.put("sub-jobs",subJobs.stream().map(SubJob::generateResponseMap).toList());
      return map;
   }
}
