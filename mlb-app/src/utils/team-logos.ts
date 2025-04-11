// Map of team names to their logo file names
// The actual logo files should be placed in the public/team-logos directory
export const teamLogoMap: Record<string, string> = {
    // American League East
    "Baltimore Orioles": "orioles.png",
    "Boston Red Sox": "red-sox.png",
    "New York Yankees": "yankees.png",
    "Tampa Bay Rays": "rays.png",
    "Toronto Blue Jays": "blue-jays.png",
  
    // American League Central
    "Chicago White Sox": "white-sox.png",
    "Cleveland Guardians": "guardians.png",
    "Detroit Tigers": "tigers.png",
    "Kansas City Royals": "royals.png",
    "Minnesota Twins": "twins.png",
  
    // American League West
    "Houston Astros": "astros.png",
    "Los Angeles Angels": "angels.png",
    "Athletics": "athletics.png",
    "Seattle Mariners": "mariners.png",
    "Texas Rangers": "rangers.png",
  
    // National League East
    "Atlanta Braves": "braves.png",
    "Miami Marlins": "marlins.png",
    "New York Mets": "mets.png",
    "Philadelphia Phillies": "phillies.png",
    "Washington Nationals": "nationals.png",
  
    // National League Central
    "Chicago Cubs": "cubs.png",
    "Cincinnati Reds": "reds.png",
    "Milwaukee Brewers": "brewers.png",
    "Pittsburgh Pirates": "pirates.png",
    "St. Louis Cardinals": "cardinals.png",
  
    // National League West
    "Arizona Diamondbacks": "diamondbacks.png",
    "Colorado Rockies": "rockies.png",
    "Los Angeles Dodgers": "dodgers.png",
    "San Diego Padres": "padres.png",
    "San Francisco Giants": "giants.png",
  }
  
  // Function to get the logo URL for a team
  export function getTeamLogoUrl(teamName: string): string {
    const logoFileName = teamLogoMap[teamName]
    if (!logoFileName) {
      console.warn(`No logo found for team: ${teamName}`)
      return "/team-logos/placeholder.png" // Fallback to a placeholder
    }
    return `/team-logos/${logoFileName}`
  }
  