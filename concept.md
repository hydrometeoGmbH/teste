# Ziel:

## Daten
* Datacube mit Sensor & Radardaten je Regenereignis.
    * dabei die Radardaten als 3d-array (lon, lat, time)
    * Die Sensordaten korrespondierend mit jeweiliger Timeseries, sowie information über lon und lat
    * alternative Ortungsdaten auch Gridposition

## Auswertung
* Auswertung der Korrelation der Timeseries in jede Dimension der Radar-Daten
    * Als Korrellationswert
* Detaillierte Auswertung des "optimalen" Ergebnis
    * --> "optimales" Ergebnis ist die Verschiebung, die im Mittel die beste Korellation hat
    * als Ergebniswürfel
        * Im Ergebniswürfel Farbliche Darstellung, je nach korellation
        * Mittelwerte der Korrelationen über das ganze Event.
        * --> hier wird das optimale Ergebnis sichtbar.
    * Sensordaten dabei "statisch" und unverändert 
        * Sensoren mit Niederschlagswert unter <X> für das Regenereignis müssen nicht ausgewertet werden
            * <X> hierbei veränderlich und am Anfang einstellbar.

## Ausgabe
* Export der Zeitreihen je Sensor und je zugehöriges Radarpixel, so dass sie visualisiert werden können (z.B. in TimeView)
    * Zeitreihen je Sensor 
        * Auswahl:
            * jede Durchgeführte Verschiebung
            * nur unverschoben, Eventoptimum und individualoptimum
    * Format: --> UVF


## Zusätzlich:
* Erkennung interessanter Ereignisse:
    * bspw. Scraper über Sensordaten --> mind. <A> Sensoren erreichen in Zeiteinheit <B> eine Klasse von <C>
    * --> Entkoppelt von der Auswertung.

