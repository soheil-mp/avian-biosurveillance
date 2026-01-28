# Avian Biosurveillance

**Tracking Bioacoustic Data to Detect Population Changes and Vocalization Anomalies for Outbreak Detection**

This project investigates the potential of passive acoustic monitoring (PAM) and automated bioacoustic analysis (specifically BirdNET) to serve as an early warning system for arboviral disease outbreaks in wild birds, with a primary focus on the **Usutu virus (USUV)** in **Eurasian blackbirds (*Turdus merula*)**.

## Executive Summary

Bioacoustics has transformed biodiversity monitoring, yet its potential for disease surveillance remains unexplored. This project tests whether data from platforms like **BirdNET** and **BirdWeather** can detect signal disruptions—such as reduced detection rates or altered vocal activity—correlated with confirmed disease outbreaks. Our research bridges the gap between ornithology, epidemiology, and machine learning to develop novel biosurveillance tools.

## Key Hypotheses

We investigate four primary signals that may indicate an outbreak:

1.  **Reduced Detection Rates**: As mortality rises, the frequency of BirdNET detections should decline proportionally.
2.  **Altered Vocal Activity**: Sick individuals may vocalize less frequently or produce altered songs due to physiological stress.
3.  **Community-Level Changes**: Multi-species die-offs should reduce overall soundscape complexity metrics (ACI, entropy).
4.  **Spatial Correlation**: Acoustic declines should track the geographic spread of confirmed USUV cases.

## Methodology

Our proposed framework consists of three phases:

1.  **Baseline Establishment**: Characterizing natural seasonal and daily variability in blackbird vocal activity using historical data and pilot deployments.
2.  **Outbreak Monitoring**: Real-time analysis of data from BirdWeather stations, comparing current `Vocal Activity Rates (VAR)` against established baselines.
3.  **Integration & Validation**: Cross-referencing acoustic anomalies with "gold standard" mortality data from and **Sovon** and **DWHC**.

## Data Sources

*   **Acoustic Data**:
    *   **BirdNET**: Deep learning algorithm for species identification (>6,000 species).
    *   **BirdWeather**: Global network of continuous acoustic monitoring stations.
*   **Epidemiological Data**:
    *   **Sovon Dutch Centre for Field Ornithology**: Citizen science mortality reporting.
    *   **Dutch Wildlife Health Centre (DWHC)** & **Erasmus MC**: Pathology and virology results (USUV status).

## System Architecture

[View System Architecture Diagram](https://app.eraser.io/workspace/mWWQKHwBUAfwXil72fNp?origin=share)

## References & Resources

### Project Resources
*   **BirdNET Algorithm**: [Article](https://www.sciencedirect.com/science/article/pii/S1574954121000273) | [Github](https://github.com/kahst/BirdNET-Analyzer) | [Website](https://birdnet.cornell.edu/)
*   **BirdNET Guidelines**: [Wood & Kahl (2024)](https://connormwood.com/wp-content/uploads/2024/02/wood-kahl-2024-guidelines-for-birdnet-scores.pdf)
*   **BirdWeather**: [Live Map](https://app.birdweather.com/)

### Disease & Mortality Data
*   **Avian Biosurveillance Review**: [Nature Communications (2025)](https://www.nature.com/articles/s41467-025-63122-w)
*   **DWHC Mortality Reports**: [Blackbird Mortality (2024)](https://dwhc.nl/en/2024/09/once-again-an-increased-mortality-of-common-blackbirds/) | [USUV Report](https://dwhc.nl/wp-content/uploads/sites/393/2024/04/57_60_DLN2_Usutu-Auteursbestand.pdf)
*   **Sovon Data**: [Mortality Database](https://portal.sovon.nl/dood/result/index/11870?jaar=2024&maand=8&oorzaak=&pid=0)