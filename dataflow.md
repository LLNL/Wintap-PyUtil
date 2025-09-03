Here's the updated dependency diagram with **batch names** included for a hierarchical structure. I’ve grouped the dependencies under the **Detail** and **Summary** batches to clearly show the flow of data and processing.

### **Updated Dependency Diagram with Batch Names**

```mermaid
graph TD 
    subgraph WintapOnly ["Wintap Only Data"]
        subgraph Detail ["Bronze"]
            raw_host --> host
            raw_process --> process
            raw_process_file --> process_file
            raw_image_load --> process_image_load
            raw_process_registry --> process_registry
            raw_process_conn_incr --> process_conn_incr
            process_conn_incr --> process_net_conn
        end

        subgraph Summary ["Silver"]
            process_net_conn --> process_net_summary
            process_registry --> process_registry_summary
            process_file --> process_file_summary
            process_image_load --> process_image_load_summary
        end

        subgraph Gold ["Gold"]
            process --> process_summary
            host --> process_summary
            process_registry_summary --> process_summary
            process_file_summary --> process_summary
            process_net_summary --> process_summary
            process_image_load_summary --> process_summary
        end
    end

    subgraph Labels ["Labels"]
        caldera --> process_uber_summary
        manual --> process_uber_summary
        mitre --> process_uber_summary
        sigma --> process_uber_summary
        process_summary --> process_uber_summary
    end
```

---

### **Annotations for the Diagram**

#### **Detail Batch**
- **`raw_process`**: The raw input data for processes.
- **`process`**: Transformed and structured data from `raw_process`.
- **`raw_process_conn_incr`**: The raw input data for process connections.
- **`process_conn_incr`**: Transformed and structured data from `raw_process_conn_incr`.
- **`process_net_conn`**: Aggregated connection-level data derived from `process_conn_incr`.

#### **Summary Batch**
- **`process_registry_summary`**: Summarized registry activity for processes (from `process_registry`).
- **`process_file_summary`**: Summarized file activity for processes (from `process_file`).
- **`process_net_summary`**: Summarized network activity for processes (from `process_net_conn`).
- **`process_image_load_summary`**: Summarized image load activity for processes (from `process_image_load`).
- **`process_summary`**: A comprehensive summary combining data from all summarized views (`process_registry_summary`, `process_file_summary`, `process_net_summary`, `process_image_load_summary`) and enriched with host-level details (`host`).

---

### **Key Highlights**
- The **Detail Batch** focuses on creating foundational tables from raw data.
- The **Summary Batch** builds higher-level summaries and combines them into a single, comprehensive `process_summary` view.
- The hierarchical structure ensures clarity in how data flows from raw inputs to detailed tables and then to summarized views.
