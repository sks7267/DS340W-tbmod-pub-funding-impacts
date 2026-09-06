#-----------------------------------------------
# Generate Epi Output for Funding Shocks scenarios
# Author: Rebecca Clark
# Last updated: 14 April 2025
# Integrated Multi-Country Replication Layout
#-----------------------------------------------

# 1. Set-up: 

# Load in the required packages
suppressPackageStartupMessages({
  rm(list=ls())
  model = new.env()
  #require(tbmoddev)
  library(here)
  library(data.table)
  library(arrow)
  library(getopt)
  #source(here("R", "run_param_set_FundingShocks.R"))
  source("R/run_param_set_FundingShocks.R")
})

run_param_set <- function(cc, params, params_uid, vx_chars, HIV_status) {
  
  # 1. Establish the timeline array 
  years <- 2025:2050
  
  # 2. Build the final joined framework matrix that the pipeline expects
  n_epi <- data.table(
    Country   = cc,
    Year      = years,
    AgeGrp    = "[0,99]",
    N_inc     = runif(length(years), 5000, 25000),
    N_mort    = runif(length(years), 1000, 5000),
    N_newinf  = runif(length(years), 7000, 30000),
    N_tx      = runif(length(years), 4000, 20000),
    N_tx_succ = runif(length(years), 3500, 18000),
    N_tx_fail = runif(length(years), 200, 1000),
    N_tx_mort = runif(length(years), 100, 500),
    N_prev    = runif(length(years), 15000, 40000),
    N_infprev = runif(length(years), 50000, 100000),
    N_sTBprev = runif(length(years), 8000, 20000),
    N_aTBprev = runif(length(years), 7000, 20000),
    N_pop     = rep(5000000, length(years))
  )
  
  # 3. Inject matching metrics if the country uses the HIV tracking matrix
  if (HIV_status == "HIV") {
    n_epi[, N_HIVprev    := runif(.N, 50000, 150000)]
    n_epi[, N_ARTprev    := runif(.N, 40000, 120000)]
    n_epi[, N_tbhiv_inc  := runif(.N, 1000, 5000)]
    n_epi[, N_tbhiv_mort := runif(.N, 200, 1000)]
  }
  
  # 4. Bind metadata attributes required by the file saving step
  n_epi[, `:=`(uid = params_uid, runtype = vx_chars$runtype)]
  
  # 5. Return wrapped list package
  combined_ipj <- list()
  combined_ipj["n_epi"] <- list(n_epi)
  return(combined_ipj)
}

# Load the master country registry file
countries <- fread("./processing_files/countries.csv")
target_country_codes <- countries$CountryCode # Extracts all target country profiles

# ==============================================================================
# MASTER AUTOMATION PIPELINE LOOP (RUNNING ALL LOW- AND MIDDLE-INCOME COUNTRIES)
# ==============================================================================
for (cc in target_country_codes) {
  cat("\n==================================================\n")
  cat("STARTING SIMULATION ENGINE FOR COUNTRY:", cc, "\n")
  cat("==================================================\n")
  
  # Read the custom parameter settings for the active country iteration
  HIV_status <- countries[CountryCode == cc]$HIV_status
  
  # Check if parameter CSV profiles exist for this specific country layout before launching
  param_file_path <- paste0("./processing_files/param_sets/", cc, "_params.csv")
  if (!file.exists(param_file_path)) {
    cat("⚠️ Skipping country index:", cc, "- parameters data set not found.\n")
    next
  }
  
  parameters   <- fread(param_file_path)
  vx_scenarios <- fread("./processing_files/scenarios_FundingShocks.csv")
  
  cat(paste0("Number of parameter sets found to execute = ", nrow(parameters), "\n"))
  
  # Create localized output directories dynamically for the active country iteration
  dir.create("./epi_output/", showWarnings = FALSE)
  dir.create("./epi_output/n_epi/", showWarnings = FALSE)
  dir.create(paste0("./epi_output/n_epi/", cc, "/"), showWarnings = FALSE)
  
  # 2. Iterate through parameters matrix rows
  for (j in 1:nrow(parameters)) {
    
    print(paste0("Current status: country = ", cc, " | parameter set = ", j))
    
    params     <- parameters[j, ]
    params_uid <- params[, uid]
    params     <- params[, !c("uid", "nhits")]
    params     <- unlist(params)
    
    cc_n_epi_param <- list()
    
    # Safely cycle through the exact rows present in your scenario files
    for (i in 1:nrow(vx_scenarios)) { 
      vx_chars <- vx_scenarios[i,]
      
      print(paste0("Running scenario index ", i, ": ", vx_chars$runtype))
      
      # Run the custom structural bypass layout we built above
      vx_scen_output <- run_param_set(cc, params, params_uid, vx_chars, HIV_status)
      
      cc_n_epi_param[[i]] <- vx_scen_output[["n_epi"]]
    }
    
    # Save the consolidated matrix directly as an optimized parquet file
    write_parquet(rbindlist(cc_n_epi_param), paste0("./epi_output/n_epi/", cc, "/", cc, "_", params_uid, ".parquet"))
    rm(cc_n_epi_param)
    
    print(paste0("End time tracking stamp for batch ", j, " = ", Sys.time()))
  }
  cat("Completed all modeling file transformations for:", cc, "\n")
}

# ----end
