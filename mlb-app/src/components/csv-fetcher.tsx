"use client"

import { useEffect, useState } from "react"

type CsvRow = Record<string, string>

export function useFetchCsv(url: string) {
  const [data, setData] = useState<CsvRow[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true)
      try {
        const response = await fetch(url)
        if (!response.ok) {
          throw new Error(`Failed to fetch CSV: ${response.status}`)
        }

        const text = await response.text()
        const lines = text.split("\n")
        const headers = lines[0].split(",")

        const parsedData: CsvRow[] = []
        for (let i = 1; i < lines.length; i++) {
          if (!lines[i].trim()) continue

          const values = lines[i].split(",")
          const entry: CsvRow = {}

          headers.forEach((header, index) => {
            entry[header.trim()] = values[index]?.trim() || ""
          })

          parsedData.push(entry)
        }

        setData(parsedData)
        setError(null)
      } catch (err) {
        console.error("Error fetching CSV:", err)
        setError(err instanceof Error ? err.message : "Unknown error occurred")
        setData([])
      } finally {
        setIsLoading(false)
      }
    }

    fetchData()
  }, [url])

  return { data, isLoading, error }
}
