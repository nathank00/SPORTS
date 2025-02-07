"use client"; // Required for client-side fetching

import { useEffect, useState } from "react";

interface Game {
  game_id: string;
  home_team: string;
  away_team: string;
  runline: string;
  pick: string;
}

export default function Home() {
  const [games, setGames] = useState<Game[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchData() {
      try {
        const res = await fetch("/api/games");
        const data = await res.json();
        setGames(data);
      } catch (error) {
        console.error("Error fetching game picks:", error);
        setGames([]);
      } finally {
        setLoading(false);
      }
    }

    fetchData();
    const interval = setInterval(fetchData, 60000); // Auto-refresh every 60 sec
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={styles.container}>
      <h1 style={styles.header}>MLB Game Picks</h1>

      {loading ? (
        <p style={styles.loading}>Loading...</p>
      ) : (
        <div style={styles.gamesContainer}>
          {games.length === 0 ? (
            <p>No picks available.</p>
          ) : (
            games.map((game, index) => (
              <div key={index} style={styles.gameCard}>
                <h3>
                  {game.home_team} vs {game.away_team}
                </h3>
                <p>
                  <strong>Runline:</strong> {game.runline}
                </p>
                <p>
                  <strong>Pick:</strong> {game.pick}
                </p>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}

// Styles
const styles = {
  container: {
    fontFamily: "Arial, sans-serif",
    textAlign: "center" as const,
    padding: "20px",
    backgroundColor: "#f4f4f4",
    minHeight: "100vh",
  },
  header: {
    color: "#333",
    fontSize: "2em",
    marginBottom: "20px",
  },
  loading: {
    fontSize: "1.5em",
    color: "#666",
  },
  gamesContainer: {
    display: "flex",
    flexWrap: "wrap" as const,
    justifyContent: "center",
    gap: "20px",
  },
  gameCard: {
    backgroundColor: "white",
    padding: "15px",
    borderRadius: "8px",
    boxShadow: "0 4px 6px rgba(0,0,0,0.1)",
    maxWidth: "300px",
  },
};
