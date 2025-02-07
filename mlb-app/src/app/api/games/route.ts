import { NextResponse } from "next/server";

export async function GET() {
  const today = new Date().toISOString().split("T")[0]; // Get today's date
  const csvUrl = `https://raw.githubusercontent.com/nathank00/MLB-Analytics/main/picks/${today}.csv`;

  try {
    const response = await fetch(csvUrl);
    if (!response.ok) throw new Error("Failed to fetch CSV");

    const text = await response.text();
    const rows = text.split("\n").slice(1); // Remove headers
    const games = rows.map((row) => {
      const [game_id, home_team, away_team, runline, pick] = row.split(",");
      return { game_id, home_team, away_team, runline, pick };
    });

    return NextResponse.json(games);
  } catch (error) {
    return NextResponse.json({ error: "Could not fetch data" }, { status: 500 });
  }
}
