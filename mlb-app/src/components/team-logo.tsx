"use client"

import { getTeamLogoUrl } from "@/utils/team-logos"
import { useState } from "react"

interface TeamLogoProps {
  teamName: string
  className?: string
}

export function TeamLogo({ teamName, className = "" }: TeamLogoProps) {
  const [showFallback, setShowFallback] = useState(false)

  if (showFallback) {
    return <span className={className}>{teamName}</span>
  }

  return (
    <img
      src={getTeamLogoUrl(teamName) || "/placeholder.svg"}
      alt={`${teamName} logo`}
      className={`inline-block h-auto ${className}`}
      onError={() => setShowFallback(true)}
    />
  )
}
