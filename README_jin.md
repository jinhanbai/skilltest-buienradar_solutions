# Remarks
# Gezien de client/database nu lokaal zijn, is er een indefinite loop geimplementeerd die de data ophaalt, transformeert en opslaat (in de weather_cont.db database).
# Echter in een cloud omgeving kan er waarschijnlijk een schedule time van de pipeline geconfigureerd worden waardoor je alleen de code hoeft te runnen wanneer nodig.
# De pipeline is declareerd in def main(), door het weather_automation.py script te runnen kan de pipeline worden afgetrapt.
