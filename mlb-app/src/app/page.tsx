"use client"
import { useEffect, useState } from "react"
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { AlertCircle, Check, Info, Calendar, RefreshCw, ExternalLink, Github } from "lucide-react"
import { useFetchCsv } from "@/components/csv-fetcher"

type GamePick = {
  game_id: string | number
  home_team: string
  away_team: string
  runline: string
  pick: string
  Away_SP_Name: string
  Home_SP_Name: string
  Away_Batter1_Name: string
  Away_Batter2_Name: string
  Away_Batter3_Name: string
  Away_Batter4_Name: string
  Away_Batter5_Name: string
  Away_Batter6_Name: string
  Away_Batter7_Name: string
  Away_Batter8_Name: string
  Away_Batter9_Name: string
  Home_Batter1_Name: string
  Home_Batter2_Name: string
  Home_Batter3_Name: string
  Home_Batter4_Name: string
  Home_Batter5_Name: string
  Home_Batter6_Name: string
  Home_Batter7_Name: string
  Home_Batter8_Name: string
  Home_Batter9_Name: string
  [key: string]: string | number
}

export default function Home() {
  const [games, setGames] = useState<GamePick[]>([])
  const [error, setError] = useState("")
  const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split("T")[0])
  const [lastUpdated, setLastUpdated] = useState<string>("Loading...")
  const [isLoading, setIsLoading] = useState(true)

  // Fetch the last_updated.csv file
  const { data: lastUpdatedData, error: lastUpdatedError } = useFetchCsv("/last_updated.csv")

useEffect(() => {
  if (lastUpdatedError) {
    console.error("Failed to fetch last updated data:", lastUpdatedError)
    setLastUpdated("Unavailable")
  } else if (lastUpdatedData && lastUpdatedData.length > 0 && lastUpdatedData[0].last_updated) {
    setLastUpdated(lastUpdatedData[0].last_updated)
  }
}, [lastUpdatedData, lastUpdatedError])

  // Update last updated value when the data is loaded
  useEffect(() => {
    if (lastUpdatedData && lastUpdatedData.length > 0 && lastUpdatedData[0].last_updated) {
      setLastUpdated(lastUpdatedData[0].last_updated)
    }
  }, [lastUpdatedData])

  const fetchPicks = (date: string) => {
    setIsLoading(true)
    fetch(`/api/games?date=${date}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.error) {
          setError(data.error)
          setGames([])
        } else {
          setError("")
          setGames(data)
        }
        setIsLoading(false)
      })
      .catch((err) => {
        console.error("Error fetching picks:", err)
        setError("Failed to load picks.")
        setGames([])
        setIsLoading(false)
      })
  }

  // Update the useEffect to default to today's date
  useEffect(() => {
    // Get current date in YYYY-MM-DD format
    const today = new Date().toISOString().split("T")[0]
    setSelectedDate(today)
    fetchPicks(today)
  }, [])

  // Replace the existing useEffect that depends on selectedDate with this:
  useEffect(() => {
    if (selectedDate) {
      fetchPicks(selectedDate)
    }
  }, [selectedDate])

  // Check if a game has complete lineup data
  const isGameBetReady = (game: GamePick): boolean => {
    // Check if both starting pitchers are available
    if (!game.Away_SP_Name || !game.Home_SP_Name) return false

    // Check if all batters are available for away team
    for (let i = 1; i <= 9; i++) {
      const batterKey = `Away_Batter${i}_Name` as keyof GamePick
      if (!game[batterKey]) return false
    }

    // Check if all batters are available for home team
    for (let i = 1; i <= 9; i++) {
      const batterKey = `Home_Batter${i}_Name` as keyof GamePick
      if (!game[batterKey]) return false
    }

    return true
  }

  // Format date for display - Fixed to handle timezone issues
  const formatDate = (dateString: string): string => {
    // Create date with explicit year, month, day to avoid timezone issues
    const [year, month, day] = dateString.split("-").map((num) => Number.parseInt(num, 10))
    const date = new Date(year, month - 1, day) // month is 0-indexed in JS Date

    const options: Intl.DateTimeFormatOptions = {
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric",
    }
    return date.toLocaleDateString(undefined, options)
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      <header className="sticky top-0 z-10 bg-slate-900 border-b border-slate-800 p-4">
        <div className="container mx-auto flex flex-col md:flex-row justify-between items-center">
          <h1 className="text-3xl font-bold text-blue-500 mb-4 md:mb-0">1 OF 1 Dashboard</h1>

          <div className="flex flex-col md:flex-row items-center gap-4">
            <div className="flex items-center gap-2 bg-slate-800 px-3 py-1 rounded-md">
              <Calendar className="h-4 w-4 text-blue-400" />
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="bg-transparent border-none text-white focus:outline-none"
              />
            </div>

            <div className="flex items-center gap-2 bg-slate-800 px-3 py-1 rounded-md">
              <RefreshCw className="h-4 w-4 text-green-400" />
              <span className="text-sm">Last updated: {lastUpdated}</span>
            </div>

            <div className="flex gap-2">
              <a
                href="https://www.cbssports.com/mlb/scoreboard/"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 text-sm text-blue-400 hover:text-blue-300"
              >
                <ExternalLink className="h-4 w-4" />
                <span>CBS Sports</span>
              </a>
              <a
                href="https://github.com/nathank00/MLB-Analytics"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 text-sm text-blue-400 hover:text-blue-300"
              >
                <Github className="h-4 w-4" />
                <span>GitHub</span>
              </a>
            </div>
          </div>
        </div>
      </header>

      <main className="container mx-auto py-8 px-4">
        <div className="mb-6">
          <h2 className="text-2xl font-semibold mb-2">Games for {formatDate(selectedDate)}</h2>
          <div className="flex items-center gap-4 text-sm">
            <div className="flex items-center gap-1">
              <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/20">
                <Check className="h-3 w-3 mr-1" /> Ready to bet
              </Badge>
            </div>
            <div className="flex items-center gap-1">
              <Badge variant="outline" className="bg-amber-500/10 text-amber-500 border-amber-500/20">
                <AlertCircle className="h-3 w-3 mr-1" /> Awaiting data
              </Badge>
            </div>
          </div>
        </div>

        {isLoading ? (
          <div className="flex justify-center items-center h-64">
            <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
          </div>
        ) : error ? (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-center">
            <p className="text-red-400">{error}</p>
          </div>
        ) : games.length === 0 ? (
          <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-8 text-center">
            <p className="text-slate-400">No games available for this date.</p>
          </div>
        ) : (
          <Tabs defaultValue="all" className="w-full">
            <TabsList className="mb-6">
              <TabsTrigger value="all">All Games ({games.length})</TabsTrigger>
              <TabsTrigger value="ready">
                Ready to Bet ({games.filter((game) => isGameBetReady(game)).length})
              </TabsTrigger>
              <TabsTrigger value="not-ready">
                Not Ready ({games.filter((game) => !isGameBetReady(game)).length})
              </TabsTrigger>
            </TabsList>

            {["all", "ready", "not-ready"].map((tab) => (
              <TabsContent key={tab} value={tab} className="mt-0">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {games
                    .filter((game) => {
                      if (tab === "all") return true
                      if (tab === "ready") return isGameBetReady(game)
                      if (tab === "not-ready") return !isGameBetReady(game)
                      return true
                    })
                    .map((game, index) => {
                      const isBetReady = isGameBetReady(game)
                      return (
                        <Card
                          key={index}
                          className={`bg-slate-900 border-slate-700 overflow-hidden ${isBetReady ? "border-l-4 border-l-green-500" : "border-l-4 border-l-amber-500"}`}
                        >
                          <CardHeader className="pb-2">
                            <div className="flex justify-between items-start">
                              <CardTitle className="text-xl font-bold">
                                {game.away_team} @ {game.home_team}
                              </CardTitle>
                              <Badge variant={isBetReady ? "success" : "warning"} className="ml-2">
                                {isBetReady ? (
                                  <span className="flex items-center">
                                    <Check className="h-3 w-3 mr-1" /> Ready
                                  </span>
                                ) : (
                                  <span className="flex items-center">
                                    <AlertCircle className="h-3 w-3 mr-1" /> Pending
                                  </span>
                                )}
                              </Badge>
                            </div>
                          </CardHeader>
                          <CardContent>
                            <div className="grid grid-cols-2 gap-4 mb-4">
                              <div className="bg-slate-800 rounded-lg p-3 text-center">
                                <div className="text-sm text-slate-400 mb-1">Live Runline</div>
                                <div className="text-xl font-bold">{game.runline}</div>
                              </div>
                              <div
                                className={`rounded-lg p-3 text-center ${game.pick === "Over" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}
                              >
                                <div className="text-sm mb-1">Monkey King says:</div>
                                <div className="text-xl font-bold">{game.pick}</div>
                              </div>
                            </div>

                            <div className="grid grid-cols-2 gap-4 text-sm">
                              <div>
                                <div className="text-slate-400 mb-1">Away SP</div>
                                <div className="truncate">{game.Away_SP_Name || "TBD"}</div>
                              </div>
                              <div>
                                <div className="text-slate-400 mb-1">Home SP</div>
                                <div className="truncate">{game.Home_SP_Name || "TBD"}</div>
                              </div>
                            </div>
                          </CardContent>
                          <CardFooter className="pt-0">
                            <Dialog>
                              <DialogTrigger asChild>
                                <Button variant="outline" className="w-full">
                                  <Info className="h-4 w-4 mr-2" />
                                  View Lineups
                                </Button>
                              </DialogTrigger>
                              <DialogContent className="bg-slate-900 border-slate-700 text-white max-w-3xl">
                                <DialogHeader>
                                  <DialogTitle className="text-xl font-bold text-center">
                                    {game.away_team} @ {game.home_team}
                                  </DialogTitle>
                                </DialogHeader>
                                <div className="mt-4">
                                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    <div className="space-y-4">
                                      <h3 className="text-lg font-semibold text-blue-400">{game.away_team} Lineup</h3>
                                      <div className="bg-slate-800 rounded-lg p-4">
                                        <div className="mb-2 font-semibold">Starting Pitcher</div>
                                        <div className="mb-4 pl-2 border-l-2 border-blue-500">
                                          {game.Away_SP_Name || "TBD"}
                                        </div>
                                        <div className="mb-2 font-semibold">Batting Order</div>
                                        <div className="space-y-1">
                                          {Array.from({ length: 9 }, (_, i) => {
                                            const batterKey = `Away_Batter${i + 1}_Name` as keyof GamePick
                                            return (
                                              <div key={i} className="flex items-center">
                                                <span className="w-6 h-6 flex items-center justify-center bg-slate-700 rounded-full text-xs mr-2">
                                                  {i + 1}
                                                </span>
                                                <span>{game[batterKey] || "TBD"}</span>
                                              </div>
                                            )
                                          })}
                                        </div>
                                      </div>
                                    </div>
                                    <div className="space-y-4">
                                      <h3 className="text-lg font-semibold text-blue-400">{game.home_team} Lineup</h3>
                                      <div className="bg-slate-800 rounded-lg p-4">
                                        <div className="mb-2 font-semibold">Starting Pitcher</div>
                                        <div className="mb-4 pl-2 border-l-2 border-blue-500">
                                          {game.Home_SP_Name || "TBD"}
                                        </div>
                                        <div className="mb-2 font-semibold">Batting Order</div>
                                        <div className="space-y-1">
                                          {Array.from({ length: 9 }, (_, i) => {
                                            const batterKey = `Home_Batter${i + 1}_Name` as keyof GamePick
                                            return (
                                              <div key={i} className="flex items-center">
                                                <span className="w-6 h-6 flex items-center justify-center bg-slate-700 rounded-full text-xs mr-2">
                                                  {i + 1}
                                                </span>
                                                <span>{game[batterKey] || "TBD"}</span>
                                              </div>
                                            )
                                          })}
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                  <div className="mt-6 p-4 bg-slate-800 rounded-lg">
                                    <div className="flex items-center justify-between">
                                      <div>
                                        <span className="text-slate-400">Runline:</span>
                                        <span className="ml-2 font-semibold">{game.runline}</span>
                                      </div>
                                      <div>
                                        <span className="text-slate-400">Prediction:</span>
                                        <span
                                          className={`ml-2 font-semibold ${game.pick === "Over" ? "text-green-400" : "text-red-400"}`}
                                        >
                                          {game.pick}
                                        </span>
                                      </div>
                                      <div>
                                        <Badge variant={isBetReady ? "success" : "warning"}>
                                          {isBetReady ? "Ready to Bet" : "Missing Data"}
                                        </Badge>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              </DialogContent>
                            </Dialog>
                          </CardFooter>
                        </Card>
                      )
                    })}
                </div>
              </TabsContent>
            ))}
          </Tabs>
        )}
      </main>
    </div>
  )
}

