import { NextRequest, NextResponse } from "next/server"
import { getXataClient } from "@/lib/xata"

const xata = getXataClient()

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url)
    const date = searchParams.get("date")

    if (!date) {
      return NextResponse.json({ error: "Missing date parameter." }, { status: 400 })
    }

    const startOfDay = new Date(`${date}T00:00:00.000Z`)
    const endOfDay = new Date(`${date}T23:59:59.999Z`)

    // Pull all games for both daily and all-time stats
    const masterGames = await xata.db.master
      .select([
        "game_id",
        "model_prediction",
        "prediction_confidence",
        "label_over_under",
        "game_complete",
        "total_runs_scored",
        "game_date",
        "description",
        "runs_home",
        "runs_away",
        "game_started"
      ])
      .getAll()

    // Daily games
    const dailyGames = masterGames.filter(
      (g): g is typeof g & { game_date: Date } =>
        g.game_date instanceof Date &&
        g.game_date >= startOfDay &&
        g.game_date <= endOfDay
    )

    const gameIds = dailyGames.map(g => g.game_id).filter(Boolean) as string[]

    // Daily accuracy
    const dailyValidGames = dailyGames.filter(g =>
      g.model_prediction !== null &&
      g.label_over_under !== null &&
      g.game_complete
    )

    const dailyWins = dailyValidGames.filter(
      g => g.model_prediction === g.label_over_under
    ).length

    const dailyLosses = dailyValidGames.length - dailyWins

    // All-time accuracy
    const allTimeValidGames = masterGames.filter(g =>
      g.model_prediction !== null &&
      g.label_over_under !== null &&
      g.game_complete
    )

    const allTimeWins = allTimeValidGames.filter(
      g => g.model_prediction === g.label_over_under
    ).length

    const allTimeLosses = allTimeValidGames.length - allTimeWins
    const allTimePercent = allTimeValidGames.length > 0
      ? `${((allTimeWins / allTimeValidGames.length) * 100).toFixed(1)}%`
      : "0.0%"

    // Get games from games table for lineup and metadata
    const games = await xata.db.games
      .select([
        "game_id", "home_name", "away_name", "start_time", "runline",
        "away_sp_id", "home_sp_id",
        "away_1_id", "away_2_id", "away_3_id", "away_4_id", "away_5_id",
        "away_6_id", "away_7_id", "away_8_id", "away_9_id",
        "home_1_id", "home_2_id", "home_3_id", "home_4_id", "home_5_id",
        "home_6_id", "home_7_id", "home_8_id", "home_9_id"
      ] as const)
      .filter({ game_id: { $any: gameIds } })
      .getAll()

    // Collect all player IDs from SPs + batters
    const playerIds = new Set<string>()
    games.forEach(g => {
      [
        g.away_sp_id, g.home_sp_id,
        g.away_1_id, g.away_2_id, g.away_3_id, g.away_4_id, g.away_5_id,
        g.away_6_id, g.away_7_id, g.away_8_id, g.away_9_id,
        g.home_1_id, g.home_2_id, g.home_3_id, g.home_4_id, g.home_5_id,
        g.home_6_id, g.home_7_id, g.home_8_id, g.home_9_id
      ].forEach(id => id && playerIds.add(id))
    })

    // Player map
    const players = await xata.db.players
      .select(["player_id", "player_name"])
      .filter({ player_id: { $any: Array.from(playerIds) } })
      .getAll()

    const playerMap = Object.fromEntries(
      players.map(p => [p.player_id ?? "", p.player_name ?? "TBD"])
    )

    type EnrichedGameData = {
      model_prediction: number | null | undefined;
      prediction_confidence: number | null;
      label_over_under: number | null | undefined;
      game_complete: boolean | null | undefined;
      runs_total: number | null;
      description: string | null;
      runs_home: number | null;
      runs_away: number | null;
      game_started: boolean;
    };

    // Build enriched map from masterGames
    const enrichedMap: Record<string, EnrichedGameData> = {}
    for (const game of masterGames) {
      const id = String(game.game_id ?? "")
      enrichedMap[id] = {
        model_prediction: game.model_prediction,
        prediction_confidence: game.prediction_confidence ?? null,
        label_over_under: game.label_over_under,
        game_complete: game.game_complete,
        runs_total: game.total_runs_scored ?? null,
        description: game.description ?? null,
        runs_home: game.runs_home ?? null,
        runs_away: game.runs_away ?? null,
        game_started: game.game_started ?? false
      }
    }

    // Merge into gamePicks
    const gamePicks = games.map(g => {
      const id = String(g.game_id ?? "")
      const enriched = enrichedMap[id] ?? {}

      return {
        game_id: g.game_id ?? "",
        home_name: g.home_name ?? "TBD",
        away_name: g.away_name ?? "TBD",
        start_time: g.start_time?.toString() ?? null,
        runline: g.runline ?? null,
        pick:
          enriched.model_prediction === 1
            ? "Over"
            : enriched.model_prediction === 0
            ? "Under"
            : "-",
        runs_total: enriched.runs_total ?? null,
        result: enriched.label_over_under ?? "TBD",
        model_prediction: enriched.model_prediction ?? null,
        prediction_confidence: enriched.prediction_confidence ?? null,
        label_over_under: enriched.label_over_under ?? null,
        game_complete: enriched.game_complete ?? false,
        description: enriched.description ?? null,
        runs_home: enriched.runs_home ?? null,
        runs_away: enriched.runs_away ?? null,
        game_started: enriched.game_started ?? false,

        Away_SP_Name: playerMap[g.away_sp_id ?? ""] ?? "TBD",
        Home_SP_Name: playerMap[g.home_sp_id ?? ""] ?? "TBD",

        Away_Batter1_Name: playerMap[g.away_1_id ?? ""] ?? "TBD",
        Away_Batter2_Name: playerMap[g.away_2_id ?? ""] ?? "TBD",
        Away_Batter3_Name: playerMap[g.away_3_id ?? ""] ?? "TBD",
        Away_Batter4_Name: playerMap[g.away_4_id ?? ""] ?? "TBD",
        Away_Batter5_Name: playerMap[g.away_5_id ?? ""] ?? "TBD",
        Away_Batter6_Name: playerMap[g.away_6_id ?? ""] ?? "TBD",
        Away_Batter7_Name: playerMap[g.away_7_id ?? ""] ?? "TBD",
        Away_Batter8_Name: playerMap[g.away_8_id ?? ""] ?? "TBD",
        Away_Batter9_Name: playerMap[g.away_9_id ?? ""] ?? "TBD",

        Home_Batter1_Name: playerMap[g.home_1_id ?? ""] ?? "TBD",
        Home_Batter2_Name: playerMap[g.home_2_id ?? ""] ?? "TBD",
        Home_Batter3_Name: playerMap[g.home_3_id ?? ""] ?? "TBD",
        Home_Batter4_Name: playerMap[g.home_4_id ?? ""] ?? "TBD",
        Home_Batter5_Name: playerMap[g.home_5_id ?? ""] ?? "TBD",
        Home_Batter6_Name: playerMap[g.home_6_id ?? ""] ?? "TBD",
        Home_Batter7_Name: playerMap[g.home_7_id ?? ""] ?? "TBD",
        Home_Batter8_Name: playerMap[g.home_8_id ?? ""] ?? "TBD",
        Home_Batter9_Name: playerMap[g.home_9_id ?? ""] ?? "TBD",
      }
    })

    return NextResponse.json({
      gamePicks,
      dailyRecord: { wins: dailyWins, losses: dailyLosses },
      allTimeRecord: {
        wins: allTimeWins,
        losses: allTimeLosses,
        percent: allTimePercent,
        total: allTimeValidGames.length
      }
    })

  } catch (err) {
    console.error("API error in /api/omega-games:", err)
    return NextResponse.json({ error: "Internal server error." }, { status: 500 })
  }
}
