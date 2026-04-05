

Main.py does

  Step 1 — Read the 300MB CSVs efficiently                                                          
  Reads only the 4 key columns (name, gender, age, exam date) from each GBK-encoded CSV in chunks of
   50,000 rows, converts to UTF-8 and saves small temp files.                                       
                                                                                                    
  Step 2 — Convert the xlsx                                 
  Reads the stroke registry Excel file and saves it as CSV.                                         
                                                                                                    
  Step 3 — Build DuckDB views                                                                       
  Creates in-memory query-able views for all 4 sources. Also normalises the gender column — the xlsx
   stores '1 男' while CSVs store '男', so it strips the number so joins work.                      
   
  Step 4 — Count records                                                                            
  Prints row counts and unique (name+gender) counts per source.
                                                                                                    
  Step 5 — Build the presence matrix ← the main output                                              
  For every unique patient across all 4 files, creates one row showing:                             
  - in_stroke / in_2022 / in_2023 / in_2024 — Y or N                                                
  - Their ages in each year                                                                         
  - age_consistent — whether ages make sense for the same person (e.g. age 50 in 2022 → expect 51 in
   2023 ±1)                                                                                         
                                                                                                    
  Step 6 — Full intersection
  Filters to patients who appear in all 4 sources with consistent ages → intersection_all4.csv      
                                                                                                    
  Step 7 — Missing records — 3 output files:                                                        
  - stroke_missing_from_csvs.csv — stroke patients absent from ≥1 annual health check               
  - csv_not_in_stroke_registry.csv — health check patients not in stroke registry                   
  - age_mismatch_warnings.csv — same name+gender but ages don't match → likely two different people
                                                                                                    
  Step 8 — Per-year breakdown                               
  Prints how many stroke patients were found/missing in each of 2022, 2023, 2024.       
  

