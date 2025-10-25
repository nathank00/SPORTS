"use client"
import { useEffect, useState, useRef, useMemo } from "react"
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  AlertCircle,
  Check,
  Info,
  Calendar,
  RefreshCw,
  ExternalLink,
  Github,
  Download,
  Clock,
  BarChart3,
  ArrowUp,
  ArrowDown,
} from "lucide-react"
import { useFetchCsv } from "@/components/csv-fetcher"
import { TeamLogo } from "@/components/team-logo"
import { useRouter } from "next/navigation"

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

type EnrichedGameData = {
  game_id: string | number
  prediction: string | number
  outcome: string | number
  completed: string | number
  runs_total: string | number
  runline: string | number
  start_time: string | number
  [key: string]: string | number
}

// Define a type for the CSV row data
type CsvRow = Record<string, string | number>

export default function Home() {
  const router = useRouter()
  const [games, setGames] = useState<GamePick[]>([])
  const [error, setError] = useState("")
  const [selectedDate, setSelectedDate] = useState(() => {
    const pacificNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" }))
    const year = pacificNow.getFullYear()
    const month = String(pacificNow.getMonth() + 1).padStart(2, "0")
    const day = String(pacificNow.getDate()).padStart(2, "0")
    return `${year}-${month}-${day}`
  })
  const [lastUpdated, setLastUpdated] = useState<string>("Loading...")
  const [isLoading, setIsLoading] = useState(true)
  const [accuracy, setAccuracy] = useState<{ wins: number; losses: number; percent: string; total: number }>({
    wins: 0,
    losses: 0,
    percent: "0.0%",
    total: 0,
  })
  const [enrichedGames, setEnrichedGames] = useState<Record<string | number, EnrichedGameData>>({})
  const [showDatePicker, setShowDatePicker] = useState(false)
  const [sidebarVisible, setSidebarVisible] = useState(true)
  const datePickerRef = useRef<HTMLDivElement>(null)

  // Fetch the last_updated.csv file
  const { data: lastUpdatedData, error: lastUpdatedError } = useFetchCsv("/last_updated.csv")

  // Fetch the cumulative_performance.csv file
  const { data: performanceData, error: performanceError } = useFetchCsv("/data/cumulative_performance.csv")

  // Fetch the enriched data for the selected date
  const { data: enrichedData, error: enrichedError } = useFetchCsv(`/data/${selectedDate}_enriched.csv`)

  useEffect(() => {
    if (lastUpdatedError) {
      console.error("Failed to fetch last updated data:", lastUpdatedError)
      setLastUpdated("Unavailable")
    } else if (lastUpdatedData && lastUpdatedData.length > 0 && lastUpdatedData[0].last_updated) {
      setLastUpdated(lastUpdatedData[0].last_updated)
    }
  }, [lastUpdatedData, lastUpdatedError])

  // Calculate accuracy from performance data
  useEffect(() => {
    if (performanceError) {
      console.error("Failed to fetch performance data:", performanceError)
      setAccuracy({
        wins: 0,
        losses: 0,
        percent: "Unavailable",
        total: 0,
      })
    } else if (performanceData && performanceData.length > 0) {
      let wins = 0
      let total = 0

      performanceData.forEach((game: CsvRow) => {
        // Only count games where both prediction and outcome are available
        if (
          game.prediction !== undefined &&
          game.outcome !== undefined &&
          game.prediction !== "" &&
          game.outcome !== ""
        ) {
          total++
          // Count as a win if prediction matches outcome
          if (game.prediction.toString() === game.outcome.toString()) {
            wins++
          }
        }
      })

      const losses = total - wins
      if (total > 0) {
        const accuracyValue = ((wins / total) * 100).toFixed(1)
        setAccuracy({
          wins,
          losses,
          percent: `${accuracyValue}%`,
          total,
        })
      } else {
        setAccuracy({
          wins: 0,
          losses: 0,
          percent: "No data",
          total: 0,
        })
      }
    }
  }, [performanceData, performanceError])

  // Process enriched data for the selected date
  useEffect(() => {
    if (enrichedError) {
      console.error(`Failed to fetch enriched data for ${selectedDate}:`, enrichedError)
    } else if (enrichedData && enrichedData.length > 0) {
      const enrichedMap: Record<string | number, EnrichedGameData> = {}

      // Use CsvRow type instead of any
      enrichedData.forEach((game: CsvRow) => {
        if (game.game_id) {
          enrichedMap[game.game_id] = game as EnrichedGameData
        }
      })

      setEnrichedGames(enrichedMap)
    } else {
      setEnrichedGames({})
    }
  }, [enrichedData, enrichedError, selectedDate])

  // Close date picker when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (datePickerRef.current && !datePickerRef.current.contains(event.target as Node)) {
        setShowDatePicker(false)
      }
    }

    document.addEventListener("mousedown", handleClickOutside)
    return () => {
      document.removeEventListener("mousedown", handleClickOutside)
    }
  }, [])

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

  // Update the useEffect to default to today's date (PST)
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

  // Determine game prediction status (W, L, TBD, push)
  const getGameStatus = (game: GamePick): { status: string; className: string; shortStatus: string } => {
    const enrichedGame = enrichedGames[game.game_id]
    if (!enrichedGame) {
      return { status: "Pending", className: "bg-teal-900/40 text-teal-300", shortStatus: "TBD" }
    }

    const prediction = enrichedGame.prediction
    const outcome = enrichedGame.outcome
    const completed = enrichedGame.completed === "1" || enrichedGame.completed === 1

    if (completed) {
      if (outcome === "push") {
        return { status: "Push", className: "bg-gray-700 text-gray-300", shortStatus: "PUSH" }
      }
      if (prediction === outcome) {
        return { status: "Winner", className: "bg-green-900/40 text-green-400", shortStatus: "W" }
      } else {
        return { status: "Loser", className: "bg-red-900/40 text-red-400", shortStatus: "L" }
      }
    } else {
      if (outcome === "push") {
        return { status: "Push (In Progress)", className: "bg-gray-700/40 text-gray-300", shortStatus: "TBD" }
      }
      if (prediction === "1" && outcome === "1") {
        return { status: "Winner (Locked)", className: "bg-green-900/30 text-green-300", shortStatus: "W" }
      }
      if (prediction === "0" && outcome === "1") {
        return { status: "Loser (Locked)", className: "bg-red-900/30 text-red-300", shortStatus: "L" }
      }
      return { status: "Pending", className: "bg-gray-800/30 text-gray-300", shortStatus: "TBD" }
    }
  }


  // Calculate daily record
  const dailyRecord = useMemo(() => {
    let wins = 0
    let losses = 0

    games.forEach((game) => {
      const status = getGameStatus(game)
      if (status.shortStatus === "W") {
        wins++
      } else if (status.shortStatus === "L") {
        losses++
      }
      // Pushes (P) and TBD are ignored
    })

    return { wins, losses }
  }, [games, enrichedGames])


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

  // Format time from 24-hour to 12-hour format
  const formatTime = (timeString: string | number): string => {
    if (!timeString) return "TBD"

    // If it's already in the right format, return it
    if (typeof timeString === "string" && (timeString.includes("AM") || timeString.includes("PM"))) {
      return timeString
    }

    try {
      // Assuming timeString is in 24-hour format like "14:30"
      const [hours, minutes] = timeString.toString().split(":").map(Number)
      const period = hours >= 12 ? "PM" : "AM"
      const formattedHours = hours % 12 || 12 // Convert 0 to 12 for 12 AM
      return `${formattedHours}:${minutes.toString().padStart(2, "0")} ${period}`
    } catch (error) {
      console.error("Error formatting time:", error)
      return timeString.toString()
    }
  }

  // Toggle sidebar visibility
  const toggleSidebar = () => {
    setSidebarVisible(!sidebarVisible)
  }

  return (
    <div className="flex min-h-screen bg-[#021414] text-teal-50 font-light">
      {/* Left Sidebar */}
      <aside
        className={`w-[26rem] bg-[#011010] border-r border-gray-700/30 p-4 flex flex-col fixed h-full transition-all duration-300 ease-in-out ${
          sidebarVisible ? "left-0" : "-left-[26rem]"
        }`}
      >

        <div className="mb-8"></div>
        <div className="mb-8 flex justify-center w-full">
          <h1
            className="text-2xl font-mono text-white mb-2 font-light tracking-wide cursor-pointer hover:text-teal-300 transition-colors whitespace-nowrap text-center"
            onClick={() => setSidebarVisible(false)}
          >
              [ALPHA]
          </h1>
        </div>

        <div className="mb-2 flex justify-center">
          <img src="/team-logos/monkeyking5.png" alt="Monkey King" className="w-24 h-auto" />
        </div>

        {/* Accuracy Display */}
        <div className="mb-14">
          <div className="flex items-center gap-2 mb-2"></div>
          <div>
            <div className="flex items-center justify-center gap-3 mb-2">
              <span className="text-green-400 font-light">{accuracy.wins} W</span>
              <span className="text-gray-500 mx-1">|</span>
              <span className="text-red-400 font-light">{accuracy.losses} L</span>
            </div>
            <div className="text-center text-teal-300/80 font-light text-sm">
              {accuracy.percent} ({accuracy.total} games)
            </div>
          </div>
        </div>

        {/* Unified Sidebar Section with Individual Boxes */}
        <div className="space-y-2">
          {/* Last Updated */}
          <div className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-3 flex items-center gap-2 text-teal-300/80 font-medium">
            <RefreshCw className="h-4 w-4" />
            <span>{lastUpdated}</span>
          </div>

          {/* External Links */}
          <a
            href="https://www.cbssports.com/mlb/scoreboard/"
            target="_blank"
            rel="noopener noreferrer"
            className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-3 flex items-center gap-2 text-teal-300/80 hover:text-teal-200 transition-colors font-medium"
          >
            <ExternalLink className="h-4 w-4" />
            <span>View MLB Games</span>
          </a>
          <a
            href="https://github.com/nathank00/MLB-Analytics"
            target="_blank"
            rel="noopener noreferrer"
            className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-3 flex items-center gap-2 text-teal-300/80 hover:text-teal-200 transition-colors font-medium"
          >
            <Github className="h-4 w-4" />
            <span>nathank00/MLB-Analytics</span>
          </a>

          {/* Downloads */}
          <a
            href={`/data/${selectedDate}_enriched.csv`}
            download
            className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-3 flex items-center gap-2 text-teal-300/80 hover:text-teal-200 transition-colors font-medium"
          >
            <Download className="h-4 w-4" />
            <span>Today&apos;s Performance Data</span>
          </a>
          <a
            href="/data/cumulative_performance.csv"
            download
            className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-3 flex items-center gap-2 text-teal-300/80 hover:text-teal-200 transition-colors font-medium"
          >
            <BarChart3 className="h-4 w-4" />
            <span>2025 Performance Data</span>
          </a>
        </div>

        {/* Footer Text */}
        <div
          className="mt-auto text-center text-gray-200 text-sm font-light cursor-pointer hover:text-teal-300 transition-colors"
          onClick={() => router.push("/")}
        >
          © 1 OF 1 INTELLIGENCE LLC
        </div>
      </aside>

      {/* Open button (plus sign) - only visible when sidebar is closed */}
      {!sidebarVisible && (
        <button
          onClick={toggleSidebar}
          className="fixed top-4 left-4 z-20 text-white hover:text-teal-300 transition-colors"
          aria-label="Open sidebar"
        >
          <div className="mt-8 text-center text-white hover:text-teal-300 text-2xl font-light">[ ]</div>
        </button>
      )}

      {/* Main Content */}
      <main className={`flex-1 p-6 transition-all duration-300 ease-in-out ${sidebarVisible ? "ml-[26rem]" : "ml-0"}`}>
        <div className="max-w-7xl mx-auto">
          {/* Date Header with Record */}
          <div className="mb-8 text-center relative">
            <div className="flex flex-col items-center">
              <div
                className="inline-flex items-center mt-4 gap-2 cursor-pointer bg-teal-900/30 px-4 py-2 rounded-lg border border-teal-800/50 hover:bg-teal-900/50 transition-colors"
                onClick={() => setShowDatePicker(!showDatePicker)}
              >
                <Calendar className="h-5 w-5 text-teal-400" />
                <h2 className="text-xl font-light text-teal-300">{formatDate(selectedDate)}</h2>
              </div>

              {/* Daily Record */}
              <div className="mt-5 flex items-center gap-3">
                <div className="flex items-center gap-1">
                  <span className="text-green-400 font-light">{dailyRecord.wins} W</span>
                  <span className="text-gray-500 mx-1">|</span>
                  <span className="text-red-400 font-light">{dailyRecord.losses} L</span>
                </div>
              </div>
            </div>

            {/* Date Picker Popup */}
            {showDatePicker && (
              <div
                ref={datePickerRef}
                className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 bg-teal-950 border border-teal-800 rounded-lg p-4 shadow-lg z-10"
              >
                <input
                  type="date"
                  value={selectedDate}
                  onChange={(e) => {
                    setSelectedDate(e.target.value)
                    setShowDatePicker(false)
                  }}
                  className="bg-teal-900/50 border border-teal-800 rounded px-3 py-2 text-teal-100 focus:outline-none focus:ring-2 focus:ring-teal-600"
                />
              </div>
            )}
          </div>

          {/* Legend */}
          <div className="flex justify-center items-center gap-4 text-sm mb-6 font-light">
            <div className="flex items-center gap-1">
              <Badge variant="outline" className="bg-teal-900/40 text-teal-300 border-teal-700/50">
                <Check className="h-3 w-3 mr-1" /> Lineups
              </Badge>
            </div>
            <div className="flex items-center gap-1">
              <Badge variant="outline" className="bg-teal-900/40 text-teal-200 border-teal-700/50">
                <AlertCircle className="h-3 w-3 mr-1" /> Pending Lineups
              </Badge>
            </div>
          </div>

          {isLoading ? (
            <div className="flex justify-center items-center h-64">
              <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-teal-400"></div>
            </div>
          ) : error ? (
            <div className="bg-red-900/20 border border-red-700/30 rounded-lg p-4 text-center">
              <p className="text-red-400 font-light">{error}</p>
            </div>
          ) : games.length === 0 ? (
            <div className="bg-teal-900/20 border border-teal-800/30 rounded-lg p-8 text-center">
              <p className="text-teal-400 font-light">No games available for this date.</p>
            </div>
          ) : (
            <Tabs defaultValue="all" className="w-full">
              <TabsList className="mb-6 bg-teal-900/40 border border-teal-800/50">
                <TabsTrigger
                  value="all"
                  className="data-[state=active]:bg-teal-800 data-[state=active]:text-teal-50 font-light"
                >
                  All Games ({games.length})
                </TabsTrigger>
                <TabsTrigger
                  value="ready"
                  className="data-[state=active]:bg-teal-800 data-[state=active]:text-teal-50 font-light"
                >
                  Valid ({games.filter((game) => isGameBetReady(game)).length})
                </TabsTrigger>
                <TabsTrigger
                  value="not-ready"
                  className="data-[state=active]:bg-teal-800 data-[state=active]:text-teal-50 font-light"
                >
                  Pending ({games.filter((game) => !isGameBetReady(game)).length})
                </TabsTrigger>
              </TabsList>

              {["all", "ready", "not-ready"].map((tab) => (
                <TabsContent key={tab} value={tab} className="mt-0">
                  <div className="grid gap-6 grid-cols-[repeat(auto-fit,minmax(320px,1fr))]">
                    {games
                      .filter((game) => {
                        if (tab === "all") return true
                        if (tab === "ready") return isGameBetReady(game)
                        if (tab === "not-ready") return !isGameBetReady(game)
                        return true
                      })
                      .map((game, index) => {
                        const isBetReady = isGameBetReady(game)
                        const gameStatus = getGameStatus(game)
                        const enrichedGame = enrichedGames[game.game_id]
                        const startTime = enrichedGame?.start_time ? formatTime(enrichedGame.start_time) : "TBD"
                        const runsTotal = enrichedGame?.runs_total || "0"

                        return (
                          <Card
                            key={index}
                            className={`bg-[#011010] border border-teal-900/50 overflow-hidden shadow-lg hover:shadow-teal-900/30 transition-shadow ${
                              isBetReady ? "border-teal-700" : "border-teal-900"
                            }`}
                          >
                            <CardHeader className="pb-2 bg-gradient-to-r from-teal-900/40 to-transparent">
                              <div className="flex justify-between items-start">
                                <CardTitle className="text-xl font-light text-teal-200 flex items-center gap-2">
                                  <div className="flex items-center gap-1">
                                    <TeamLogo teamName={game.away_team} className="h-12 w-12" />
                                    <span className="mx-1">@</span>
                                    <TeamLogo teamName={game.home_team} className="h-12 w-12" />
                                  </div>
                                </CardTitle>
                                <Badge
                                  className={`ml-2 ${
                                    isBetReady
                                      ? "bg-teal-800/60 text-teal-300 hover:bg-teal-800/80"
                                      : "bg-teal-900/60 text-teal-200 hover:bg-teal-900/80"
                                  }`}
                                >
                                  {isBetReady ? (
                                    <span className="flex items-center">
                                      <Check className="h-3 w-3 mr-1" /> Lineups
                                    </span>
                                  ) : (
                                    <span className="flex items-center">
                                      <AlertCircle className="h-3 w-3 mr-1" /> Pending Lineups
                                    </span>
                                  )}
                                </Badge>
                              </div>
                              {/* Game Time */}
                              <div className="flex items-center gap-2 mt-2 text-teal-400 text-sm font-light">
                                <Clock className="h-4 w-4" />
                                <span>{startTime}</span>
                              </div>
                            </CardHeader>
                            <CardContent className="pt-4">
                              {/* 4-Tile Layout */}
                              <div className="grid grid-cols-2 gap-3 mb-4">
                                {/* Runline - Upper Left */}
                                <div className="bg-teal-900/30 rounded-lg p-3 text-center border border-teal-900/30">
                                  <div className="text-sm text-teal-400 mb-1 font-light">Runline</div>
                                  <div className="text-xl font-light text-teal-200">{game.runline}</div>
                                </div>

                                {/* Prediction - Upper Right */}
                                <div
                                  className={`rounded-lg p-3 text-center border ${
                                    game.pick === "Over"
                                      ? "bg-teal-800/30 text-teal-300 border-teal-800/30"
                                      : "bg-teal-900/30 text-teal-400 border-teal-900/30"
                                  }`}
                                >
                                  <div className="text-sm mb-1 font-light">Prediction</div>
                                  <div className="text-xl flex justify-center items-center">
                                    {game.pick === "Over" ? (
                                      <ArrowUp className="h-6 w-6 text-teal-300" />
                                    ) : (
                                      <ArrowDown className="h-6 w-6 text-teal-400" />
                                    )}
                                  </div>
                                </div>

                                {/* Runs Total - Lower Left */}
                                <div className="bg-teal-900/30 rounded-lg p-3 text-center border border-teal-900/30">
                                  <div className="text-sm text-teal-400 mb-1 font-light">Live Runs</div>
                                  <div className="text-xl font-light text-teal-200">{runsTotal}</div>
                                </div>

                                {/* Game Status - Lower Right */}
                                <div
                                  className={`rounded-lg p-3 text-center border ${gameStatus.className} border-opacity-30 flex items-center justify-center`}
                                >
                                  <div className="text-2xl font-light">{gameStatus.shortStatus}</div>
                                </div>
                              </div>
                            </CardContent>
                            <CardFooter className="pt-0">
                              <Dialog>
                                <DialogTrigger asChild>
                                  <Button
                                    variant="outline"
                                    className="w-full bg-teal-900/40 text-teal-300 border-teal-900/50 hover:bg-teal-800/60 hover:text-teal-100 font-light"
                                  >
                                    <Info className="h-4 w-4 mr-2" />
                                    View Lineups
                                  </Button>
                                </DialogTrigger>
                                <DialogContent className="bg-[#011010] border-teal-900 text-teal-50 max-w-3xl">
                                  <DialogHeader>
                                    <DialogTitle className="text-xl font-light text-center text-teal-300 flex items-center justify-center gap-2">
                                      <TeamLogo teamName={game.away_team} className="h-16 w-16" />
                                      <span>@</span>
                                      <TeamLogo teamName={game.home_team} className="h-16 w-16" />
                                    </DialogTitle>
                                  </DialogHeader>
                                  <div className="mt-4">
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                      <div className="space-y-4">
                                        <h3 className="text-lg font-light text-teal-400">{game.away_team} Lineup</h3>
                                        <div className="bg-teal-900/30 rounded-lg p-4 border border-teal-900/30">
                                          <div className="mb-2 font-light text-teal-300">Starting Pitcher</div>
                                          <div className="mb-4 pl-2 border-l-2 border-teal-700 text-teal-200 font-light">
                                            {game.Away_SP_Name || "TBD"}
                                          </div>
                                          <div className="mb-2 font-light text-teal-300">Batting Order</div>
                                          <div className="space-y-1">
                                            {Array.from({ length: 9 }, (_, i) => {
                                              const batterKey = `Away_Batter${i + 1}_Name` as keyof GamePick
                                              return (
                                                <div key={i} className="flex items-center">
                                                  <span className="w-6 h-6 flex items-center justify-center bg-teal-800 rounded-full text-xs mr-2 text-teal-200">
                                                    {i + 1}
                                                  </span>
                                                  <span className="text-teal-200 font-light">
                                                    {game[batterKey] || "TBD"}
                                                  </span>
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </div>
                                      </div>
                                      <div className="space-y-4">
                                        <h3 className="text-lg font-light text-teal-400">{game.home_team} Lineup</h3>
                                        <div className="bg-teal-900/30 rounded-lg p-4 border border-teal-900/30">
                                          <div className="mb-2 font-light text-teal-300">Starting Pitcher</div>
                                          <div className="mb-4 pl-2 border-l-2 border-teal-700 text-teal-200 font-light">
                                            {game.Home_SP_Name || "TBD"}
                                          </div>
                                          <div className="mb-2 font-light text-teal-300">Batting Order</div>
                                          <div className="space-y-1">
                                            {Array.from({ length: 9 }, (_, i) => {
                                              const batterKey = `Home_Batter${i + 1}_Name` as keyof GamePick
                                              return (
                                                <div key={i} className="flex items-center">
                                                  <span className="w-6 h-6 flex items-center justify-center bg-teal-800 rounded-full text-xs mr-2 text-teal-200">
                                                    {i + 1}
                                                  </span>
                                                  <span className="text-teal-200 font-light">
                                                    {game[batterKey] || "TBD"}
                                                  </span>
                                                </div>
                                              )
                                            })}
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                    <div className="mt-6 p-4 bg-teal-900/30 rounded-lg border border-teal-900/30">
                                      <div className="flex flex-col md:flex-row items-center justify-between gap-4">
                                        <div>
                                          <span className="text-teal-400">Runline:</span>
                                          <span className="ml-2 font-light text-teal-200">{game.runline}</span>
                                        </div>
                                        <div>
                                          <span className="text-teal-400">Prediction:</span>
                                          <span
                                            className={`ml-2 font-light ${
                                              game.pick === "Over" ? "text-teal-300" : "text-teal-400"
                                            }`}
                                          >
                                            {game.pick}
                                          </span>
                                        </div>
                                        <div>
                                          <Badge
                                            className={
                                              isBetReady
                                                ? "bg-teal-800/60 text-teal-200"
                                                : "bg-teal-900/60 text-teal-300"
                                            }
                                          >
                                            {isBetReady ? "Valid" : "Pending Lineups"}
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
        </div>
      </main>
    </div>
  )
}
