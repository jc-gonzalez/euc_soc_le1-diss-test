# euc_soc_le1-diss-test - LE1 DSS Dissemination Test

The scripts in this project are in charge of generating LE1 VIS/NIR/SIR
Products, along with their appropriate metadata XML files, and ingest
them into the DSS.

Another tool will be in charge of monitoring the appearance of these
products into the EAS, in order to prepare the necessary Data
Dissemination Orders (DDO) that will be sent to the different SDCs  
(depending on a pre-defined mapping plan). The SDCs, upon reception of
the DDOs, will then request the retrieval of the corresponding files.

